from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class WithdrawArgs(typing.TypedDict):
    withdraw_lamports: int


layout = borsh.CStruct("withdraw_lamports" / borsh.U64)


class WithdrawAccounts(typing.TypedDict):
    from_: Pubkey
    destination: Pubkey
    manager: Pubkey


def withdraw(
    args: WithdrawArgs,
    accounts: WithdrawAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["from_"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["destination"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["manager"], is_signer=True, is_writable=True),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b'\xb7\x12F\x9c\x94m\xa1"'
    encoded_args = layout.build(
        {
            "withdraw_lamports": args["withdraw_lamports"],
        }
    )
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
