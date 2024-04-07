from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class UpdateFeeArgs(typing.TypedDict):
    comet_fee: int
    hop_planet_fee: int
    hop_star_fee2: int
    hop_star_fee3: int
    withdraw_fee: int


layout = borsh.CStruct(
    "comet_fee" / borsh.U32,
    "hop_planet_fee" / borsh.U32,
    "hop_star_fee2" / borsh.U32,
    "hop_star_fee3" / borsh.U32,
    "withdraw_fee" / borsh.U32,
)


class UpdateFeeAccounts(typing.TypedDict):
    universe: Pubkey
    creator: Pubkey


def update_fee(
    args: UpdateFeeArgs,
    accounts: UpdateFeeAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["universe"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["creator"], is_signer=True, is_writable=True),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\xe8\xfd\xc3\xf7\x94\xd4I\xde"
    encoded_args = layout.build(
        {
            "comet_fee": args["comet_fee"],
            "hop_planet_fee": args["hop_planet_fee"],
            "hop_star_fee2": args["hop_star_fee2"],
            "hop_star_fee3": args["hop_star_fee3"],
            "withdraw_fee": args["withdraw_fee"],
        }
    )
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
