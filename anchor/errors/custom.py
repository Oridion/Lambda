import typing
from anchorpy.error import ProgramError


class CometRdmOEError(ProgramError):
    def __init__(self) -> None:
        super().__init__(6000, "Rdm oe error")

    code = 6000
    name = "CometRdmOEError"
    msg = "Rdm oe error"


class CometRdmBrError(ProgramError):
    def __init__(self) -> None:
        super().__init__(6001, "Rdm br error")

    code = 6001
    name = "CometRdmBrError"
    msg = "Rdm br error"


class PlanetDeleteHasFundsError(ProgramError):
    def __init__(self) -> None:
        super().__init__(6002, "Planet cannot be deleted. Has funds")

    code = 6002
    name = "PlanetDeleteHasFundsError"
    msg = "Planet cannot be deleted. Has funds"


class CometIdLengthError(ProgramError):
    def __init__(self) -> None:
        super().__init__(6003, "Comet id length error")

    code = 6003
    name = "CometIdLengthError"
    msg = "Comet id length error"


class HopErrorFromPlanetNotCorrect(ProgramError):
    def __init__(self) -> None:
        super().__init__(6004, "From planet is not the same")

    code = 6004
    name = "HopErrorFromPlanetNotCorrect"
    msg = "From planet is not the same"


class HopErrorToAndFromAreSame(ProgramError):
    def __init__(self) -> None:
        super().__init__(6005, "To and from cannot be the same")

    code = 6005
    name = "HopErrorToAndFromAreSame"
    msg = "To and from cannot be the same"


class HopErrorStarsMustBeUnique(ProgramError):
    def __init__(self) -> None:
        super().__init__(6006, "Stars IDs must be unique")

    code = 6006
    name = "HopErrorStarsMustBeUnique"
    msg = "Stars IDs must be unique"


class PlanetNotEnoughFundsError(ProgramError):
    def __init__(self) -> None:
        super().__init__(
            6007, "Planet does not have enough lamports to cover transaction!"
        )

    code = 6007
    name = "PlanetNotEnoughFundsError"
    msg = "Planet does not have enough lamports to cover transaction!"


class StarHopCalculationError(ProgramError):
    def __init__(self) -> None:
        super().__init__(6008, "Star split calculations do not add up!")

    code = 6008
    name = "StarHopCalculationError"
    msg = "Star split calculations do not add up!"


CustomError = typing.Union[
    CometRdmOEError,
    CometRdmBrError,
    PlanetDeleteHasFundsError,
    CometIdLengthError,
    HopErrorFromPlanetNotCorrect,
    HopErrorToAndFromAreSame,
    HopErrorStarsMustBeUnique,
    PlanetNotEnoughFundsError,
    StarHopCalculationError,
]
CUSTOM_ERROR_MAP: dict[int, CustomError] = {
    6000: CometRdmOEError(),
    6001: CometRdmBrError(),
    6002: PlanetDeleteHasFundsError(),
    6003: CometIdLengthError(),
    6004: HopErrorFromPlanetNotCorrect(),
    6005: HopErrorToAndFromAreSame(),
    6006: HopErrorStarsMustBeUnique(),
    6007: PlanetNotEnoughFundsError(),
    6008: StarHopCalculationError(),
}


def from_code(code: int) -> typing.Optional[CustomError]:
    maybe_err = CUSTOM_ERROR_MAP.get(code)
    if maybe_err is None:
        return None
    return maybe_err
