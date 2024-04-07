import typing
from dataclasses import dataclass
from construct import Construct
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Commitment
import borsh_construct as borsh
from anchorpy.coder.accounts import ACCOUNT_DISCRIMINATOR_SIZE
from anchorpy.error import AccountInvalidDiscriminator
from anchorpy.utils.rpc import get_multiple_accounts
from anchorpy.borsh_extension import BorshPubkey
from ..program_id import PROGRAM_ID


class UniverseJSON(typing.TypedDict):
    pda: str
    p: list[str]
    st: int
    up: int
    bp: int
    cfe: int
    hpfe: int
    hsfe2: int
    hsfe3: int
    wfe: int


@dataclass
class Universe:
    discriminator: typing.ClassVar = b"Vp\xe3\xe2X/\xf2q"
    layout: typing.ClassVar = borsh.CStruct(
        "pda" / BorshPubkey,
        "p" / borsh.Vec(typing.cast(Construct, borsh.String)),
        "st" / borsh.I64,
        "up" / borsh.I64,
        "bp" / borsh.U8,
        "cfe" / borsh.U64,
        "hpfe" / borsh.U64,
        "hsfe2" / borsh.U64,
        "hsfe3" / borsh.U64,
        "wfe" / borsh.U64,
    )
    pda: Pubkey
    p: list[str]
    st: int
    up: int
    bp: int
    cfe: int
    hpfe: int
    hsfe2: int
    hsfe3: int
    wfe: int

    @classmethod
    async def fetch(
        cls,
        conn: AsyncClient,
        address: Pubkey,
        commitment: typing.Optional[Commitment] = None,
        program_id: Pubkey = PROGRAM_ID,
    ) -> typing.Optional["Universe"]:
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
    ) -> typing.List[typing.Optional["Universe"]]:
        infos = await get_multiple_accounts(conn, addresses, commitment=commitment)
        res: typing.List[typing.Optional["Universe"]] = []
        for info in infos:
            if info is None:
                res.append(None)
                continue
            if info.account.owner != program_id:
                raise ValueError("Account does not belong to this program")
            res.append(cls.decode(info.account.data))
        return res

    @classmethod
    def decode(cls, data: bytes) -> "Universe":
        if data[:ACCOUNT_DISCRIMINATOR_SIZE] != cls.discriminator:
            raise AccountInvalidDiscriminator(
                "The discriminator for this account is invalid"
            )
        dec = Universe.layout.parse(data[ACCOUNT_DISCRIMINATOR_SIZE:])
        return cls(
            pda=dec.pda,
            p=dec.p,
            st=dec.st,
            up=dec.up,
            bp=dec.bp,
            cfe=dec.cfe,
            hpfe=dec.hpfe,
            hsfe2=dec.hsfe2,
            hsfe3=dec.hsfe3,
            wfe=dec.wfe,
        )

    def to_json(self) -> UniverseJSON:
        return {
            "pda": str(self.pda),
            "p": self.p,
            "st": self.st,
            "up": self.up,
            "bp": self.bp,
            "cfe": self.cfe,
            "hpfe": self.hpfe,
            "hsfe2": self.hsfe2,
            "hsfe3": self.hsfe3,
            "wfe": self.wfe,
        }

    @classmethod
    def from_json(cls, obj: UniverseJSON) -> "Universe":
        return cls(
            pda=Pubkey.from_string(obj["pda"]),
            p=obj["p"],
            st=obj["st"],
            up=obj["up"],
            bp=obj["bp"],
            cfe=obj["cfe"],
            hpfe=obj["hpfe"],
            hsfe2=obj["hsfe2"],
            hsfe3=obj["hsfe3"],
            wfe=obj["wfe"],
        )
