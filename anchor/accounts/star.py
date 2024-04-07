import typing
from dataclasses import dataclass
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Commitment
import borsh_construct as borsh
from anchorpy.coder.accounts import ACCOUNT_DISCRIMINATOR_SIZE
from anchorpy.error import AccountInvalidDiscriminator
from anchorpy.utils.rpc import get_multiple_accounts
from anchorpy.borsh_extension import BorshPubkey
from ..program_id import PROGRAM_ID


class StarJSON(typing.TypedDict):
    id: str
    amount: int
    manager: str


@dataclass
class Star:
    discriminator: typing.ClassVar = b"\xd6\x83\xcf\xd0\xca\x94\xa20"
    layout: typing.ClassVar = borsh.CStruct(
        "id" / borsh.String, "amount" / borsh.U64, "manager" / BorshPubkey
    )
    id: str
    amount: int
    manager: Pubkey

    @classmethod
    async def fetch(
        cls,
        conn: AsyncClient,
        address: Pubkey,
        commitment: typing.Optional[Commitment] = None,
        program_id: Pubkey = PROGRAM_ID,
    ) -> typing.Optional["Star"]:
        resp = await conn.get_account_info(address, commitment=commitment)
        info = resp.value
        if info is None:
            return None
        if info.owner != program_id:
            raise ValueError("Account does not belong to this program")
        bytes_data = info.data
        return cls.decode(bytes_data)

    @classmethod
    async def fetch_multiple(
        cls,
        conn: AsyncClient,
        addresses: list[Pubkey],
        commitment: typing.Optional[Commitment] = None,
        program_id: Pubkey = PROGRAM_ID,
    ) -> typing.List[typing.Optional["Star"]]:
        infos = await get_multiple_accounts(conn, addresses, commitment=commitment)
        res: typing.List[typing.Optional["Star"]] = []
        for info in infos:
            if info is None:
                res.append(None)
                continue
            if info.account.owner != program_id:
                raise ValueError("Account does not belong to this program")
            res.append(cls.decode(info.account.data))
        return res

    @classmethod
    def decode(cls, data: bytes) -> "Star":
        if data[:ACCOUNT_DISCRIMINATOR_SIZE] != cls.discriminator:
            raise AccountInvalidDiscriminator(
                "The discriminator for this account is invalid"
            )
        dec = Star.layout.parse(data[ACCOUNT_DISCRIMINATOR_SIZE:])
        return cls(
            id=dec.id,
            amount=dec.amount,
            manager=dec.manager,
        )

    def to_json(self) -> StarJSON:
        return {
            "id": self.id,
            "amount": self.amount,
            "manager": str(self.manager),
        }

    @classmethod
    def from_json(cls, obj: StarJSON) -> "Star":
        return cls(
            id=obj["id"],
            amount=obj["amount"],
            manager=Pubkey.from_string(obj["manager"]),
        )
