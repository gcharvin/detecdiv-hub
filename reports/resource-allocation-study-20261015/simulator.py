"""Offline what-if model; no Hub imports, network, subprocesses or OS quota changes.

Times are simulated minutes, work is one-reader minutes. This is not a
prediction of real job runtimes. A cooperative checkpoint explicitly releases
all resources and preserves completed work before the scheduler reallocates.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
import argparse
import json
import math
from pathlib import Path
import random
import statistics


@dataclass(frozen=True)
class Capacity:
    memory: float = 96
    cpu: int = 36
    gpu: float = 24
    io: int = 12


@dataclass(frozen=True)
class Job:
    id: str
    user: str
    arrival: float
    work: float
    minimum: float = 24
    preferred: float = 48
    maximum: float = 96
    cpu: int = 8
    gpu: float = 8
    io: int = 3
    serial: float = 0.6
    cooperative: bool = True
    flat: bool = False


@dataclass
class State:
    job: Job
    remaining: float
    allocation: float = 0
    start: float | None = None
    finish: float | None = None
    ready: float = 0
    boundary: float = math.inf
    checkpoints: int = 0


def readers(memory: float) -> int:
    # Alex's observed admission formula: 3 raw buffers, ROI/drift/client reserve,
    # 2.1875 GiB per read worker, 8 CPU ceiling. Simplified fixed 1.5-GiB RSS.
    usable = 0.8 * max(0, 0.9 * memory - 1.5)
    return max(1, min(8, math.floor((usable - 13.43) / 2.1875)))


def speed(job: Job, memory: float, storage_share: float = math.inf) -> float:
    if job.flat:
        return 1.0
    effective = max(0.01, min(readers(memory), storage_share))
    return 1.0 / (job.serial + (1 - job.serial) / effective)


def usage(active: list[State]) -> dict[str, float]:
    return {"memory": sum(s.allocation for s in active),
            "cpu": sum(s.job.cpu for s in active),
            "gpu": sum(s.job.gpu for s in active),
            "io": sum(s.job.io for s in active)}


def fits(active: list[State], job: Job, memory: float, cap: Capacity,
         reserved: tuple[Job, float] | None = None) -> bool:
    current = usage(active)
    demand = {"memory": memory, "cpu": job.cpu, "gpu": job.gpu, "io": job.io}
    if any(current[k] + demand[k] > getattr(cap, k) + 1e-8 for k in demand):
        return False
    if reserved:
        other, mem = reserved
        # Match the Hub's conservative headroom backfill: protect RAM/CPU/I/O;
        # a CPU-only backfill job does not consume a waiting job's GPU reserve.
        for k, amount in [("memory", mem), ("cpu", other.cpu), ("io", other.io)]:
            if current[k] + demand[k] + amount > getattr(cap, k) + 1e-8:
                return False
        if job.gpu and other.gpu and current["gpu"] + job.gpu + other.gpu > cap.gpu:
            return False
    return True


def user_shares(active: list[State], cap: Capacity) -> dict[str, float]:
    users: dict[str, dict[str, float]] = {}
    for s in active:
        u = users.setdefault(s.job.user, dict(memory=0, cpu=0, gpu=0, io=0))
        for k, v in dict(memory=s.allocation, cpu=s.job.cpu, gpu=s.job.gpu,
                         io=s.job.io).items():
            u[k] += v
    return {u: max(v / getattr(cap, k) for k, v in amounts.items()
                   if getattr(cap, k) > 0) for u, amounts in users.items()}


POLICIES = ("fixed24", "preferred48", "greedy_max", "elastic_fifo", "elastic_fair")


def simulate(jobs: list[Job], policy: str, cap: Capacity = Capacity(), *,
             quantum: float = 10, checkpoint_cost: float = 0.25,
             storage_readers: float = math.inf, dt: float = 0.25,
             release_only_on_contention: bool = True,
             trace: bool = False) -> dict:
    assert policy in POLICIES
    if not jobs or dt <= 0 or quantum <= 0 or checkpoint_cost < 0:
        raise ValueError("Invalid simulation parameters")
    if len({j.id for j in jobs}) != len(jobs):
        raise ValueError("Duplicate job ID")
    for j in jobs:
        if not 0 < j.minimum <= j.preferred <= j.maximum or j.work <= 0:
            raise ValueError("Invalid job resource range")
        if j.minimum > cap.memory or j.cpu > cap.cpu or j.gpu > cap.gpu or j.io > cap.io:
            raise ValueError(f"Unschedulable minimum for {j.id}")
    elastic = policy.startswith("elastic")
    states = [State(j, j.work, ready=j.arrival) for j in jobs]
    history = {j.user: 0.0 for j in jobs}
    now = 0.0
    steps = 0
    peak = dict(memory=0.0, cpu=0, gpu=0.0, io=0)
    samples = []
    events = []
    area_memory = 0.0
    while any(s.finish is None for s in states):
        steps += 1
        if steps > 200_000:
            raise RuntimeError("Simulation failed to converge")
        current_active = [s for s in states if s.allocation]
        waiting = [s for s in states if s.finish is None and not s.allocation
                   and s.ready <= now + 1e-8]
        contention = any(not fits(current_active, s.job,
                                  s.job.minimum if s.job.cooperative else s.job.preferred,
                                  cap) for s in waiting)
        # A checkpoint is a completed release acknowledgment, not a forced
        # decrease of an in-use cgroup memory limit.
        for s in states:
            if s.allocation and s.remaining <= 1e-8:
                s.finish = now
                events.append(dict(time=now, job=s.job.id, event="finish"))
                s.allocation = 0
            elif s.allocation and elastic and s.job.cooperative and now + 1e-8 >= s.boundary:
                if release_only_on_contention and not contention:
                    s.boundary = now + quantum
                    continue
                events.append(dict(time=now, job=s.job.id, event="checkpoint_release",
                                   memory=s.allocation))
                s.allocation = 0
                s.ready = now + checkpoint_cost
                s.checkpoints += 1
        if all(s.finish is not None for s in states):
            break
        active = [s for s in states if s.allocation]
        pending = [s for s in states if s.finish is None and not s.allocation and s.ready <= now + 1e-8]
        if policy == "elastic_fair":
            # History-based fair share inspired by dominant-resource accounting;
            # this is NOT an implementation of DRF's continuous-allocation proof.
            pending.sort(key=lambda s: (history[s.job.user], s.job.arrival, s.job.id))
        else:
            pending.sort(key=lambda s: (s.job.arrival, s.job.id))
        reserved = None
        new = []
        for s in pending:
            j = s.job
            if policy == "fixed24":
                mem = max(j.minimum, min(24, j.maximum))
            elif policy == "greedy_max":
                mem = min(j.maximum, cap.memory)
            elif policy == "preferred48" or not j.cooperative:
                mem = j.preferred
            else:
                mem = j.minimum
            if fits(active, j, mem, cap, reserved):
                s.allocation = mem
                s.boundary = now + quantum
                if s.start is None:
                    s.start = now
                active.append(s)
                new.append(s)
                events.append(dict(time=now, job=j.id, event="admit", memory=mem))
            elif reserved is None:
                reserved = (j, mem)
        if elastic:
            # Extra RAM is granted only with modeled marginal speedup, after
            # admitting feasible minima. Grants stay immutable until release.
            while True:
                choices = []
                for s in new:
                    if not s.job.cooperative:
                        continue
                    levels = sorted({s.job.preferred, s.job.maximum, 32, 48, 64, 96})
                    for level in levels:
                        if not s.allocation < level <= s.job.maximum:
                            continue
                        delta = level - s.allocation
                        if usage(active)["memory"] + delta > cap.memory + 1e-8:
                            continue
                        if reserved and usage(active)["memory"] + delta + reserved[1] > cap.memory + 1e-8:
                            continue
                        gain = (speed(s.job, level) - speed(s.job, s.allocation)) / delta
                        if gain > 1e-8:
                            choices.append((gain, -history[s.job.user], s.job.id, s, level))
                if not choices:
                    break
                _, _, _, chosen, level = max(choices, key=lambda c: c[:3])
                chosen.allocation = level
        totals = usage(active)
        for k, v in totals.items():
            assert v <= getattr(cap, k) + 1e-8, (k, v, cap)
            peak[k] = max(peak[k], v)
        if trace:
            samples.append(dict(time=now, **totals,
                                allocations={s.job.id: s.allocation for s in active}))
        area_memory += totals["memory"] * dt
        shares = user_shares(active, cap)
        for user, share in shares.items():
            history[user] += share * dt
        reader_total = sum(readers(s.allocation) for s in active if not s.job.flat)
        for s in active:
            share = math.inf
            if math.isfinite(storage_readers) and not s.job.flat:
                share = storage_readers * readers(s.allocation) / max(1, reader_total)
            s.remaining -= speed(s.job, s.allocation, share) * dt
        now = round(now + dt, 8)
    waits = [s.start - s.job.arrival for s in states]
    responses = [s.finish - s.job.arrival for s in states]
    result = dict(policy=policy, makespan=now,
                  mean_wait=statistics.mean(waits), max_wait=max(waits),
                  mean_response=statistics.mean(responses),
                  p95_response=sorted(responses)[max(0, math.ceil(0.95*len(responses))-1)],
                  memory_utilization=area_memory / (max(now, dt) * cap.memory),
                  checkpoints=sum(s.checkpoints for s in states), peak=peak,
                  jobs=[dict(id=s.job.id, user=s.job.user, arrival=s.job.arrival,
                             start=s.start, finish=s.finish,
                             response=s.finish-s.job.arrival) for s in states])
    if trace:
        result.update(trace=samples, events=events)
    return result


def workloads(seed: int) -> dict[str, list[Job]]:
    rng = random.Random(seed)
    base = [Job("alex", "alex", 0, 240),
            Job("other1", "other", 15, 40), Job("other2", "third", 20, 40)]
    mixed = []
    arrival = 0.0
    for i in range(18):
        arrival += rng.expovariate(1/7)
        mixed.append(Job(str(i), ["alex", "other", "third"][i % 3], arrival,
                         rng.uniform(25, 120), serial=rng.choice([0.15, 0.6, 0.9])))
    return {
        "one_job": base[:1],
        "late_arrivals": base,
        "memory_flat": [Job(**{**asdict(j), "flat": True}) for j in base],
        "legacy_no_checkpoint": [Job(**{**asdict(j), "cooperative": False}) for j in base],
        "many_jobs_one_user": [Job(f"a{i}", "alex", 0, 60) for i in range(9)] +
                              [Job("b", "other", 10, 30)],
        "mixed": mixed,
        "large_minimum": [Job("small", "alex", 0, 80),
                          Job("large", "other", 5, 60, minimum=64, preferred=64,
                              maximum=64, flat=True), Job("later", "third", 10, 10)],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--seeds", type=int, default=12)
    args = parser.parse_args()
    results = []
    for memory in [96, 78]:
        cap = Capacity(memory=memory)
        for storage, storage_label in [(math.inf, "no_shared_io_ceiling"), (2, "shared_io_ceiling_2")]:
            for seed in range(args.seeds):
                for name, jobs in workloads(seed).items():
                    for policy in POLICIES:
                        r = simulate(jobs, policy, cap, storage_readers=storage)
                        r.update(workload=name, seed=seed, capacity=asdict(cap),
                                 storage_model=storage_label)
                        results.append(r)
    sensitivity = []
    for quantum in [2, 10, 30]:
        for cost in [0, 0.25, 2]:
            r = simulate(workloads(0)["late_arrivals"], "elastic_fair",
                         quantum=quantum, checkpoint_cost=cost)
            r.update(quantum=quantum, checkpoint_cost=cost)
            sensitivity.append(r)
    naive = simulate(workloads(0)["one_job"], "elastic_fair",
                     quantum=2, checkpoint_cost=2, release_only_on_contention=False)
    conditional = simulate(workloads(0)["one_job"], "elastic_fair",
                           quantum=2, checkpoint_cost=2)
    args.out.write_text(json.dumps(dict(
        disclaimer="Synthetic offline simulation; virtual minutes are not real runtime predictions.",
        actual_profile=dict(cpu_capacity=36, gpu_vram_gib=24, io_units=12,
                            host_pool_gib=96, conservative_worker_overhead_gib=18,
                            admission_gib=78),
        results=results, sensitivity=sensitivity,
        conditional_release_comparison={"mandatory_release": naive,
                                        "only_when_contended": conditional}),
                              indent=2), encoding="utf-8")
    print(json.dumps({"simulations": len(results), "sensitivity_cases": len(sensitivity),
                      "out": str(args.out)}))


if __name__ == "__main__":
    main()
