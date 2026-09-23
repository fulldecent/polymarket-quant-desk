"""One ticket, one or two legs. Same object for FOK, GTC, one-sided, two-sided."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from explorations.frame.clip import COOLDOWN_BLOCKS


class Outcome(str, Enum):
    YES = "yes"
    NO = "no"


class Action(str, Enum):
    BUY = "buy"
    SELL = "sell"


class Tif(str, Enum):
    FOK = "fok"
    FAK = "fak"
    GTC = "gtc"
    GTD = "gtd"


class Intent(str, Enum):
    TAKE = "take"  # walk asks (buy) or bids (sell) as taker
    MAKE = "make"  # rest; fill when the other side takes


class CapTouch(str, Enum):
    INCLUSIVE = "inclusive"  # px at the signed cap counts (FOK worst price)
    STRICT = "strict"  # must be strictly through the cap (no time priority / inside)


class ShareNeed(str, Enum):
    AT_LEAST = "at_least"
    STRICTLY_MORE = "strictly_more"


class TicketKind(str, Enum):
    ONE_SIDED = "one_sided"  # buy one outcome, sell that same token
    TWO_SIDED = "two_sided"  # buy YES and NO; merge if both fill


@dataclass(frozen=True)
class Leg:
    outcome: Outcome
    action: Action
    tif: Tif
    intent: Intent
    cap: float
    n: float
    start_lag: int
    end_lag: int
    cap_touch: CapTouch
    share_need: ShareNeed = ShareNeed.AT_LEAST


@dataclass(frozen=True)
class Ticket:
    """Signed at signal block S. Features use block ≤ S only. No book re-lookup."""

    signal_block: int
    condition_id: str
    kind: TicketKind
    legs: tuple[Leg, ...]
    cooldown_blocks: int = COOLDOWN_BLOCKS

    def entry_legs(self) -> tuple[Leg, ...]:
        return tuple(lg for lg in self.legs if lg.action is Action.BUY)

    def exit_legs(self) -> tuple[Leg, ...]:
        return tuple(lg for lg in self.legs if lg.action is Action.SELL)


def one_sided_take_then_make(
    *,
    signal_block: int,
    condition_id: str,
    outcome: Outcome,
    n: float,
    p_entry: float,
    p_exit: float,
    entry_lag: int = 1,
    exit_start: int = 2,
    exit_end: int = 60,
) -> Ticket:
    """Directional: FOK buy at S+1 (cap-touch counts), GTC sell S+2..S+exit_end.

    First rest block is STRICT in the walk (no time priority); later blocks
    INCLUSIVE. That split is applied by the simulator, not stored per-block
    on the ticket — the exit leg is signed INCLUSIVE.
    """
    return Ticket(
        signal_block=signal_block,
        condition_id=condition_id,
        kind=TicketKind.ONE_SIDED,
        legs=(
            Leg(
                outcome, Action.BUY, Tif.FOK, Intent.TAKE, p_entry, n,
                entry_lag, entry_lag, CapTouch.INCLUSIVE, ShareNeed.AT_LEAST,
            ),
            Leg(
                outcome, Action.SELL, Tif.GTC, Intent.MAKE, p_exit, n,
                exit_start, exit_end, CapTouch.INCLUSIVE, ShareNeed.AT_LEAST,
            ),
        ),
    )


def two_sided_take_pair(
    *,
    signal_block: int,
    condition_id: str,
    n_yes: float,
    n_no: float,
    p_yes: float,
    p_no: float,
    aggressive_lag: int = 1,
    complement_end: int = 30,
) -> Ticket:
    """Complementary: two BUY takes, STRICT cap-touch, strictly-more shares.

    Aggressive YES in S+1 only. Complement NO in S+1..S+complement_end,
    modeled as a take-window on asks (not as maker-bid fills). Merge if
    both fill; leftover is inventory.
    """
    return Ticket(
        signal_block=signal_block,
        condition_id=condition_id,
        kind=TicketKind.TWO_SIDED,
        legs=(
            Leg(
                Outcome.YES, Action.BUY, Tif.FOK, Intent.TAKE, p_yes, n_yes,
                aggressive_lag, aggressive_lag, CapTouch.STRICT,
                ShareNeed.STRICTLY_MORE,
            ),
            Leg(
                Outcome.NO, Action.BUY, Tif.GTC, Intent.TAKE, p_no, n_no,
                aggressive_lag, complement_end, CapTouch.STRICT,
                ShareNeed.STRICTLY_MORE,
            ),
        ),
    )


def two_sided_make_pair(
    *,
    signal_block: int,
    condition_id: str,
    n_yes: float,
    n_no: float,
    p_yes: float,
    p_no: float,
    start_lag: int = 1,
    end_lag: int = 120,
) -> Ticket:
    """Rest bids on both outcomes (wide-spread maker). Fill = MAKE."""
    return Ticket(
        signal_block=signal_block,
        condition_id=condition_id,
        kind=TicketKind.TWO_SIDED,
        legs=(
            Leg(
                Outcome.YES, Action.BUY, Tif.GTC, Intent.MAKE, p_yes, n_yes,
                start_lag, end_lag, CapTouch.STRICT, ShareNeed.AT_LEAST,
            ),
            Leg(
                Outcome.NO, Action.BUY, Tif.GTC, Intent.MAKE, p_no, n_no,
                start_lag, end_lag, CapTouch.STRICT, ShareNeed.AT_LEAST,
            ),
        ),
    )
