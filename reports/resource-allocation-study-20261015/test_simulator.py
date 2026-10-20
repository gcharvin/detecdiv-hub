import unittest

from simulator import Capacity, Job, State, fits, readers, simulate, speed, user_shares


class SchedulerInvariants(unittest.TestCase):
    def test_all_policies_respect_all_resources_and_arrivals(self):
        jobs = [Job(str(i), str(i % 3), i*2, 20) for i in range(8)]
        for policy in ["fixed24", "preferred48", "greedy_max", "elastic_fifo", "elastic_fair"]:
            r = simulate(jobs, policy, trace=True)
            for row in r["trace"]:
                self.assertLessEqual(row["memory"], 96)
                self.assertLessEqual(row["cpu"], 36)
                self.assertLessEqual(row["gpu"], 24)
                self.assertLessEqual(row["io"], 12)
            self.assertTrue(all(j["start"] >= j["arrival"] for j in r["jobs"]))

    def test_no_memory_reclaim_before_explicit_checkpoint(self):
        jobs = [Job("a", "alice", 0, 100), Job("b", "bob", 1, 10)]
        r = simulate(jobs, "elastic_fair", Capacity(memory=48), quantum=10, trace=True)
        a = [row["allocations"]["a"] for row in r["trace"] if row["time"] < 10
             and "a" in row["allocations"]]
        self.assertEqual(len(set(a)), 1)
        self.assertTrue(any(e["event"] == "checkpoint_release" for e in r["events"]))

    def test_legacy_process_is_never_shrunk_or_preempted(self):
        r = simulate([Job("a", "alice", 0, 100, cooperative=False),
                      Job("b", "bob", 1, 10)], "elastic_fair", quantum=2, trace=True)
        a = [row["allocations"]["a"] for row in r["trace"] if "a" in row["allocations"]]
        self.assertEqual(set(a), {48})
        self.assertFalse(any(e["job"] == "a" and e["event"] == "checkpoint_release"
                             for e in r["events"]))

    def test_more_ram_is_not_automatically_a_speedup(self):
        j = Job("a", "a", 0, 20, flat=True)
        self.assertEqual(speed(j, 24), speed(j, 96))
        r = simulate([j], "elastic_fair")
        self.assertEqual(r["peak"]["memory"], 24)
        self.assertLessEqual(readers(96), 8)
        self.assertEqual(readers(48), readers(96))

    def test_no_restart_cost_without_competing_demand(self):
        job = Job('alone','alice',0,100)
        r = simulate([job],'elastic_fair',quantum=2,checkpoint_cost=2)
        self.assertEqual(r['checkpoints'],0)
        self.assertEqual(r['makespan'],simulate([job],'preferred48')['makespan'])

    def test_gpu_and_io_still_block_memory_rich_jobs(self):
        active = [State(Job(str(i), str(i), 0, 10), 10, allocation=24) for i in range(3)]
        self.assertFalse(fits(active, Job("x", "x", 0, 10), 24, Capacity()))
        active = active[:2]
        self.assertFalse(fits(active, Job("x", "x", 0, 10), 24, Capacity(io=8)))

    def test_fairness_aggregates_by_user_not_job_count(self):
        active = [State(Job("a1", "alice", 0, 10), 10, allocation=24),
                  State(Job("a2", "alice", 0, 10), 10, allocation=24),
                  State(Job("b", "bob", 0, 10), 10, allocation=24)]
        shares = user_shares(active, Capacity())
        self.assertAlmostEqual(shares["alice"], 2*shares["bob"])

    def test_unschedulable_minimum_and_bad_requests_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unschedulable minimum"):
            simulate([Job("x", "x", 0, 10, minimum=96, preferred=96)], "fixed24", Capacity(memory=78))
        with self.assertRaises(ValueError):
            simulate([Job("x", "x", 0, 10, minimum=48, preferred=24)], "fixed24")

    def test_late_job_gets_a_turn_under_contention(self):
        jobs = [Job(f"a{i}", "alice", 0, 100) for i in range(8)] + [Job("b", "bob", 1, 10)]
        fixed = simulate(jobs, "fixed24")
        elastic = simulate(jobs, "elastic_fair", quantum=5)
        start = lambda r: next(j["start"] for j in r["jobs"] if j["id"] == "b")
        self.assertLess(start(elastic), start(fixed))

    def test_backfill_preserves_memory_for_waiting_large_job(self):
        a = State(Job("a", "alice", 0, 10), 10, allocation=24)
        small = Job("b", "bob", 1, 10, minimum=8, preferred=8, maximum=8, cpu=1, gpu=0, io=1)
        big = Job("c", "c", 0, 10, minimum=64, preferred=64, maximum=64)
        self.assertTrue(fits([a], small, 8, Capacity(memory=90)))
        self.assertFalse(fits([a], small, 8, Capacity(memory=90), (big, 64)))


if __name__ == "__main__":
    unittest.main()
