# export interface TraderClass {
#   proxyWallet: string
#   global_roi_pct: number
#   consistency_rating: number
#   recency_weighted_pnl: number
#   trader_class: string
#   calculated_at: string
# }

from pydantic import BaseModel
from typing import Optional


class TraderClass(BaseModel):
    proxyWallet: str
    global_roi_pct: float
    consistency_rating: float
    recency_weighted_pnl: float
    trader_class: str
    calculated_at: str


class Trade(BaseModel):
    asset: str
    bio: str
    conditionId: str
    eventSlug: str
    icon: str
    name: str
    outcome: str
    outcomeIndex: str
    price: str
    profileImage: str
    proxyWallet: str
    pseudonym: str
    side: str
    size: str
    slug: str
    timestamp: str
    title: str
    transactionHash: str
    topic: str
    type: str
    messageTimestamp: str
    connection_id: str
    rawType: str


class TradeEntry(BaseModel):
    id: str
    trade: Trade

    @classmethod
    def decode(cls, data: tuple[str, dict]) -> "TradeEntry":
        """
        Decode a tuple of (id, trade_dict) into a TradeEntry.

        Args:
            data: A tuple containing (id_string, trade_dictionary)

        Returns:
            TradeEntry instance
        """
        entry_id, trade_dict = data
        trade = Trade(**trade_dict)
        return cls(id=entry_id, trade=trade)

    @classmethod
    def decode_batch(cls, data: list[tuple[str, dict]]) -> list["TradeEntry"]:
        """
        Decode a list of (id, trade_dict) tuples into a list of TradeEntry objects.

        Args:
            data: A list of tuples, each containing (id_string, trade_dictionary)

        Returns:
            List of TradeEntry instances
        """
        return [cls.decode(entry) for entry in data]

    @classmethod
    def decode_stream_message(
        cls, message: tuple[str, list[tuple[str, dict]]]
    ) -> list["TradeEntry"]:
        """
        Decode a Redis stream message format: (stream_name, [(id, trade_dict), ...])

        Args:
            message: A tuple containing (stream_name, list_of_trade_entries)

        Returns:
            List of TradeEntry instances
        """
        stream_name, entries = message
        return cls.decode_batch(entries)
