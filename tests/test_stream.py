"""Correctness tests for the streaming path."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analytics import Analytics, compare, compute_batch, stream_with_reorder
from src.bars import build_batch, build_streaming, parity
from src.multisymbol import MultiSymbolProcessor
from src.replay import DisorderConfig, Tick, generate_session, replay


@pytest.fixture(scope="module")
def session():
    cfg = DisorderConfig()
    truth = generate_session(n_ticks=20_000)
    arrivals, gaps, surviving = replay(truth, cfg)
    return cfg, truth, arrivals, surviving


def test_duplicates_are_suppressed_against_replay_ground_truth(session):
    cfg, _truth, arrivals, surviving = session
    _bars, _late, stats = build_streaming(arrivals, 60_000, cfg.watermark_bound_ms)
    assert stats.duplicates_suppressed == len(arrivals) - len(surviving)


def test_gaps_are_detected(session):
    cfg, _t, arrivals, _s = session
    _bars, _late, stats = build_streaming(arrivals, 60_000, cfg.watermark_bound_ms)
    assert len(stats.gaps_detected) == cfg.gap_count


def test_bars_adjacent_to_a_gap_are_flagged_suspect(session):
    """A quality flag ON the data. A consumer must be able to tell 'quiet
    market' from 'we were blind'."""
    cfg, _t, arrivals, _s = session
    bars, _late, stats = build_streaming(arrivals, 60_000, cfg.watermark_bound_ms)
    assert any(b.suspect for b in bars.values())


def test_every_streaming_batch_mismatch_is_explained(session):
    """The parity claim is not 'zero mismatches' -- it is 'every mismatch is a
    documented beyond-bound late arrival, and every one is flagged'."""
    cfg, _t, arrivals, surviving = session
    stream_bars, _late, _stats = build_streaming(arrivals, 60_000, cfg.watermark_bound_ms)
    batch_bars = build_batch(surviving, 60_000)
    res = parity(stream_bars, batch_bars)
    unexplained = [k for k, s, _b in res["mismatched"]
                   if not (s.suspect and "late tick" in s.suspect_reason)]
    assert not unexplained, "streaming path is wrong on {} bars".format(len(unexplained))


def test_reorder_buffer_fixes_order_dependent_metrics(session):
    """Realised vol and jump flags are functions of the SEQUENCE of returns.
    Naive appending fabricates spurious returns; the reorder buffer must not."""
    cfg, _t, arrivals, surviving = session
    naive = Analytics()
    seen = set()
    for t in arrivals:
        ident = (t.seq, t.event_time_ms)
        if ident in seen:
            continue
        seen.add(ident)
        naive.update(t)

    fixed, _dropped = stream_with_reorder(arrivals, cfg.watermark_bound_ms)
    batch = compute_batch(surviving)

    naive_err = abs(naive.snapshot()["realised_vol"] - batch["realised_vol"])
    fixed_err = abs(fixed.snapshot()["realised_vol"] - batch["realised_vol"])
    assert fixed_err < naive_err / 10, "reorder buffer did not fix ordering"
    assert fixed.snapshot()["n_jumps"] == batch["n_jumps"]


def test_vwap_is_order_independent(session):
    """The control: VWAP is a ratio of two sums, so if IT disagrees the tick
    populations differ -- the maths is not the suspect."""
    cfg, _t, arrivals, surviving = session
    naive = Analytics()
    seen = set()
    for t in arrivals:
        ident = (t.seq, t.event_time_ms)
        if ident in seen:
            continue
        seen.add(ident)
        naive.update(t)
    batch = compute_batch(surviving)
    assert naive.snapshot()["vwap_minor"] == pytest.approx(batch["vwap_minor"], rel=1e-9)


def test_idle_timeout_bounds_a_quiet_symbols_staleness():
    """Without it, a symbol that stops trading never finalises another bar."""
    proc = MultiSymbolProcessor(idle_timeout_ms=10_000)
    base = 1_800_000_000_000
    for i in range(50):
        proc.ingest("BUSY", Tick(i, base + i * 1_000, 100_00, 1), base + i * 1_000)
    proc.ingest("QUIET", Tick(999, base, 100_00, 1), base)
    # Push the busy symbol far ahead in wall-clock terms.
    for i in range(50, 200):
        proc.ingest("BUSY", Tick(i, base + i * 1_000, 100_00, 1), base + i * 1_000)

    rep = proc.stall_report()
    assert rep["idle_advances"] > 0, "idle timeout never fired"
    assert rep["stall_ms"] <= 10_000 + 5_000 + 1_000, \
        "quiet symbol drifted further behind than the idle timeout allows"


def test_per_key_watermarks_are_independent():
    proc = MultiSymbolProcessor()
    base = 1_800_000_000_000
    proc.ingest("A", Tick(0, base, 100_00, 1), base)
    proc.ingest("B", Tick(1, base + 60_000, 100_00, 1), base + 60_000)
    assert proc.states["A"].watermark != proc.states["B"].watermark
