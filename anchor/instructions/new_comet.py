from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.system_program import ID as SYS_PROGRAM_ID
from solders.sysvar import RENT
from solders.instruction import Instruction, AccountMeta
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class NewCometArgs(typing.TypedDict):
    deposit_lamports: int


layout = borsh.CStruct("deposit_lamports" / borsh.U64)


class NewCometAccounts(typing.TypedDict):
    universe: Pubkey
    planet: Pubkey
    treasury: Pubkey
    creator: Pubkey


def new_comet(
    args: NewCometArgs,
    accounts: NewCometAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["universe"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["planet"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["treasury"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["creator"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=SYS_PROGRAM_ID, is_signer=False, is_writable=False),
        AccountMeta(pubkey=RENT, is_signer=False, is_writable=False),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"x\x12\x1e\xfc\xabh\x80\xbd"
    encoded_args = layout.build(
        {
            "deposit_lamports": args["deposit_lamports"],
        }
    )
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
