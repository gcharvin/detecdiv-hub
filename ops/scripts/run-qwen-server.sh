#!/usr/bin/env bash
set -euo pipefail

: "${DETECDIV_QWEN_VLLM_BIN:?DETECDIV_QWEN_VLLM_BIN must be set}"
: "${DETECDIV_QWEN_MODEL:?DETECDIV_QWEN_MODEL must be set}"

# FlashInfer 0.6 currently emits an nvcc flag unavailable in the host's CUDA
# 12.0 toolkit.  vLLM's built-in sampler remains GPU accelerated and avoids
# that optional runtime compilation.
export PATH="$(dirname "${DETECDIV_QWEN_VLLM_BIN}"):${PATH}"
export VLLM_USE_FLASHINFER_SAMPLER="${VLLM_USE_FLASHINFER_SAMPLER:-0}"

exec "${DETECDIV_QWEN_VLLM_BIN}" serve "${DETECDIV_QWEN_MODEL}" \
  --host "${DETECDIV_QWEN_HOST:-127.0.0.1}" \
  --port "${DETECDIV_QWEN_PORT:-8001}" \
  --max-model-len "${DETECDIV_QWEN_MAX_MODEL_LEN:-8192}" \
  --gpu-memory-utilization "${DETECDIV_QWEN_GPU_MEMORY_UTILIZATION:-0.80}" \
  --max-num-seqs "${DETECDIV_QWEN_MAX_NUM_SEQS:-1}"
