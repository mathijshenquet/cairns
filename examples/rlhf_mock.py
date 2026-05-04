"""Mock RLHF pipeline.

SFT → rollout pairs → human preference labeling → reward model → PPO → eval.
Everything's mocked (deterministic hashing stands in for ML), but the shape
is real, and the labeling step is genuinely interactive.

What to notice:

- `collect_preferences` fans out `label_pair` across prompts; each spawns
  two rate-limited `generate` calls (shared "GPU queue") and then asks
  the human to pick A or B via `await_choice`. The TUI mounts
  side-by-side panels; headless mode prompts on stdin.
- Expensive stages (`sft_train`, `train_reward_model`, every `ppo_step`)
  are `@step(memo=True)` — a crash mid-PPO resumes from the last finished
  iteration, not from scratch.
- `cost={"gpu_hours": ..., "tokens": ...}` trace kwargs roll up the span
  tree so the root shows total simulated compute.
- `trace(progress=(i, n))` on the PPO loop feeds the TUI's progress bar.

Run with the TUI:

    cairn examples/rlhf_mock.py

Headless (stdin prompts for A/B):

    python examples/rlhf_mock.py
"""

from __future__ import annotations

import asyncio
import hashlib
import random

from cairns import rate_limited, run, step, trace
from cairns.interaction import await_choice


# ── Dataset + mock generation ──

PROMPTS = [
    "Haiku about debugging at 3am",
    "Explain transformers to a ten-year-old",
    "Stoic pep talk for someone who missed a deadline",
    "Limerick about legacy code",
    "Kubernetes in one paragraph",
    "Motivational quote about flaky tests",
    "Story hook: the last programmer on Earth",
    "Describe blue to someone who's never seen colour",
]

SFT_BASE = "base-7b"

# Style pool picks two distinct completions per prompt, driven by seed.
_STYLES: list[tuple[str, str]] = [
    ("earnest",   "heartfelt, plain language, lands the point"),
    ("playful",   "witty, a little wordplay, doesn't take itself too seriously"),
    ("technical", "precise, dense with jargon, assumes expertise"),
    ("poetic",    "image-rich, rhythmic, slightly overwrought"),
    ("blunt",     "three sentences, no filler, zero adjectives"),
    ("rambling",  "meandering, self-interrupting, lots of asides"),
]


def _mock_completion(checkpoint: str, prompt: str, seed: int) -> str:
    """Deterministic synthetic completion."""
    h = int(hashlib.md5(f"{checkpoint}|{prompt}|{seed}".encode()).hexdigest(), 16)
    style_name, style_desc = _STYLES[h % len(_STYLES)]
    quality = (h // 7) % 100
    return (
        f"[{style_name}, quality={quality}]\n"
        f"{style_desc}\n\n"
        f"— {checkpoint} sample #{seed}"
    )


# ── SFT ──


@step(memo=True)
async def sft_train(base: str, epochs: int) -> str:
    """Pretend to supervised-fine-tune the base model on demonstration data."""
    trace(
        f"SFT: {base} × {epochs} epochs",
        state="running",
        cost={"gpu_hours": epochs * 0.5},
    )
    await asyncio.sleep(0.15)
    tag = hashlib.md5(f"{base}-sft-{epochs}".encode()).hexdigest()[:8]
    return f"{base}-sft-{tag}"


# ── Rollouts + scoring (rate-limited shared pool) ──


@rate_limited(n=4, memo=True)
async def generate(checkpoint: str, prompt: str, seed: int) -> str:
    """Rollout one completion. Rate-limited to simulate a 4-wide GPU queue."""
    trace("generating", cost={"tokens": 200})
    await asyncio.sleep(random.uniform(0.05, 0.2))
    return _mock_completion(checkpoint, prompt, seed)


@rate_limited(n=4, memo=True)
async def score(rm: str, prompt: str, completion: str) -> float:
    """Reward model forward pass. Shares the GPU pool with generate()."""
    h = int(hashlib.md5(f"{rm}|{prompt}|{completion}".encode()).hexdigest(), 16)
    trace("scoring", cost={"tokens": 50})
    await asyncio.sleep(0.02)
    # Map to [-1, 1]; "newer" checkpoints hash higher on average.
    base = ((h % 1000) / 1000.0) * 2.0 - 1.0
    if "ppo" in rm or "ppo" in completion:
        base += 0.15
    return round(max(-1.0, min(1.0, base)), 3)


# ── Preference labeling — the HITL centerpiece ──


@step
async def label_pair(checkpoint: str, prompt: str) -> dict[str, str]:
    """Sample two completions and ask the human which one is better."""
    a = generate(checkpoint, prompt, seed=0)
    b = generate(checkpoint, prompt, seed=1)
    text_a = await a
    text_b = await b

    pick = await await_choice(
        f"Which completion is better?\n\nPrompt: {prompt}",
        options={"A": text_a, "B": text_b},
    )
    trace(f"preferred {pick}")
    return {
        "prompt": prompt,
        "chosen":   text_a if pick == "A" else text_b,
        "rejected": text_b if pick == "A" else text_a,
    }


@step(memo=True)
async def collect_preferences(
    checkpoint: str, prompts: list[str]
) -> list[dict[str, str]]:
    """Fan out labeling across prompts — N humans-in-the-loop, in parallel."""
    trace(f"collecting preferences on {len(prompts)} prompts")
    handles = [label_pair(checkpoint, p) for p in prompts]
    return [await h for h in handles]


# ── Reward model + PPO ──


@step(memo=True)
async def train_reward_model(prefs: list[dict[str, str]]) -> str:
    """Pretend to train a reward model on the labeled preference pairs."""
    trace(
        f"training RM on {len(prefs)} pairs",
        state="running",
        cost={"gpu_hours": 0.8},
    )
    await asyncio.sleep(0.2)
    h = hashlib.md5(str(sorted((p["prompt"], p["chosen"]) for p in prefs)).encode()).hexdigest()[:8]
    return f"rm-{h}"


@step(memo=True)
async def ppo_step(
    policy: str, rm: str, prompts: list[str], iteration: int
) -> tuple[str, float]:
    """One PPO iteration. Each is its own memoized step — crash mid-train, resume mid-train."""
    trace(f"PPO iteration {iteration}", state="running", cost={"gpu_hours": 0.3})

    rollouts = [generate(policy, p, seed=100 + iteration) for p in prompts]
    samples = [await r for r in rollouts]
    score_handles = [score(rm, p, s) for p, s in zip(prompts, samples)]
    scores = [await h for h in score_handles]

    mean = sum(scores) / len(scores)
    new_policy = f"{policy}-ppo{iteration}"
    trace(f"mean reward {mean:+.3f}", progress=(iteration + 1, 4))
    return new_policy, mean


@step(memo=True)
async def ppo_train(sft: str, rm: str, prompts: list[str], n_steps: int) -> str:
    policy = sft
    for i in range(n_steps):
        policy, mean = await ppo_step(policy, rm, prompts, i)
        trace(
            f"step {i}: reward={mean:+.3f}",
            progress=(i + 1, n_steps),
            edge=True,
        )
    return policy


# ── Eval ──


@step
async def evaluate(
    final_policy: str, sft: str, rm: str, prompts: list[str]
) -> dict[str, float]:
    """Compare final policy vs SFT baseline under the reward model. No humans required."""
    trace("running eval (RM-based, no humans)")

    async def compare(prompt: str) -> tuple[float, float]:
        fin_gen = generate(final_policy, prompt, seed=999)
        sft_gen = generate(sft, prompt, seed=999)
        fs = score(rm, prompt, await fin_gen)
        ss = score(rm, prompt, await sft_gen)
        return await fs, await ss

    results = [await compare(p) for p in prompts]
    wins = sum(1 for f, s in results if f > s)
    mean_final = sum(f for f, _ in results) / len(results)
    mean_sft = sum(s for _, s in results) / len(results)
    trace(
        f"win rate {wins}/{len(prompts)}",
        detail=f"final mean {mean_final:+.3f} vs SFT mean {mean_sft:+.3f}",
    )
    return {
        "win_rate": wins / len(prompts),
        "mean_final": mean_final,
        "mean_sft": mean_sft,
    }


# ── Pipeline ──


@step
async def rlhf_pipeline() -> dict[str, object]:
    """End-to-end mock RLHF: SFT → preferences → RM → PPO → eval."""
    trace("starting RLHF pipeline")

    sft = await sft_train(SFT_BASE, epochs=2)
    prefs = await collect_preferences(sft, PROMPTS)
    rm = await train_reward_model(prefs)
    final = await ppo_train(sft, rm, PROMPTS, n_steps=4)
    metrics = await evaluate(final, sft, rm, PROMPTS)

    trace("pipeline complete")
    return {"checkpoint": final, **metrics}


main = rlhf_pipeline


if __name__ == "__main__":
    from cairns.interaction import StdinInteractionSink

    print(f"Mock RLHF over {len(PROMPTS)} prompts — expect {len(PROMPTS)} A/B questions.\n")
    out = run(rlhf_pipeline, store_path=".cairn", interaction_sink=StdinInteractionSink())

    print("\n─── Results ───")
    print(f"  final checkpoint : {out['checkpoint']}")
    print(f"  win rate vs SFT  : {out['win_rate']:.0%}")
    print(f"  mean reward      : final {out['mean_final']:+.3f}   sft {out['mean_sft']:+.3f}")
    print("\nRe-run: `cairn examples/rlhf_mock.py` — every answer replays from cache.")
