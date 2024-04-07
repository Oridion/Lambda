from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.system_program import ID as SYS_PROGRAM_ID
from solders.sysvar import RENT
from solders.instruction import Instruction, AccountMeta
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class StarHopTwoStartArgs(typing.TypedDict):
    star_one: str
    star_two: str
    deposit: int


layout = borsh.CStruct(
    "star_one" / borsh.String, "star_two" / borsh.String, "deposit" / borsh.U64
)


class StarHopTwoStartAccounts(typing.TypedDict):
    from_: Pubkey
    star_one: Pubkey
    star_two: Pubkey
    manager: Pubkey


def star_hop_two_start(
    args: StarHopTwoStartArgs,
    accounts: StarHopTwoStartAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["from_"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["star_one"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["star_two"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["manager"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=RENT, is_signer=False, is_writable=False),
        AccountMeta(pubkey=SYS_PROGRAM_ID, is_signer=False, is_writable=False),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\xf4\xd0\xe4\xdf[j]\x11"
    encoded_args = layout.build(
        {
            "star_one": args["star_one"],
            "star_two": args["star_two"],
            "deposit": args["deposit"],
        }
    )
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
