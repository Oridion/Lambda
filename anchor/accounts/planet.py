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


class PlanetJSON(typing.TypedDict):
    name: str
    pda: str
    created: int
    bump: int
    visits: int


@dataclass
class Planet:
    discriminator: typing.ClassVar = b"\xf2\x1b\xec*\xdc\xd9\x84\x80"
    layout: typing.ClassVar = borsh.CStruct(
        "name" / borsh.String,
        "pda" / BorshPubkey,
        "created" / borsh.I64,
        "bump" / borsh.U8,
        "visits" / borsh.U64,
    )
    name: str
    pda: Pubkey
    created: int
    bump: int
    visits: int

    @classmethod
    async def fetch(
        cls,
        conn: AsyncClient,
        address: Pubkey,
        commitment: typing.Optional[Commitment] = None,
        program_id: Pubkey = PROGRAM_ID,
    ) -> typing.Optional["Planet"]:
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
    ) -> typing.List[typing.Optional["Planet"]]:
        infos = await get_multiple_accounts(conn, addresses, commitment=commitment)
        res: typing.List[typing.Optional["Planet"]] = []
        for info in infos:
            if info is None:
                res.append(None)
                continue
            if info.account.owner != program_id:
                raise ValueError("Account does not belong to this program")
            res.append(cls.decode(info.account.data))
        return res

    @classmethod
    def decode(cls, data: bytes) -> "Planet":
        if data[:ACCOUNT_DISCRIMINATOR_SIZE] != cls.discriminator:
            raise AccountInvalidDiscriminator(
                "The discriminator for this account is invalid"
            )
        dec = Planet.layout.parse(data[ACCOUNT_DISCRIMINATOR_SIZE:])
        return cls(
            name=dec.name,
            pda=dec.pda,
            created=dec.created,
            bump=dec.bump,
            visits=dec.visits,
        )

    def to_json(self) -> PlanetJSON:
        return {
            "name": self.name,
            "pda": str(self.pda),
            "created": self.created,
            "bump": self.bump,
            "visits": self.visits,
        }

    @classmethod
    def from_json(cls, obj: PlanetJSON) -> "Planet":
        return cls(
            name=obj["name"],
            pda=Pubkey.from_string(obj["pda"]),
            created=obj["created"],
            bump=obj["bump"],
            visits=obj["visits"],
        )
