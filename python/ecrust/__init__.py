from . import _api_compat as _api_compat
from . import _ecrust as _eccodes
from ._ecrust import *  # noqa: F401,F403
from types import ModuleType

bindings_version = __version__
eccodes = _eccodes
CODES_GRIB_NEAREST_SAME_GRID = 1
CODES_GRIB_NEAREST_SAME_DATA = 2
CODES_GRIB_NEAREST_SAME_POINT = 4

Message = CodesHandle
GRIBMessage = CodesHandle
BUFRMessage = CodesHandle
FileReader = object
StreamReader = object
MemoryReader = object

WrongTypeError = InvalidTypeError
WrongConversionError = InvalidTypeError
KeyValueNotFoundError = KeyValueNotFoundError
InvalidKeyValueError = CodesInternalError
MissingKeyError = KeyValueNotFoundError
EndError = EndOfFileError
EndOfIndexError = EndOfFileError
PrematureEndOfFileError = EndOfFileError
CorruptedIndexError = InvalidIndexError
NullIndexError = InvalidIndexError
InvalidIteratorError = InvalidKeysIteratorError
InvalidFileError = CodesInternalError
InvalidArgumentError = CodesInternalError
InvalidGribError = CodesInternalError
InternalError = CodesInternalError
GribInternalError = CodesInternalError
IOProblemError = CodesInternalError
DecodingError = CodesInternalError
EncodingError = CodesInternalError
FunctionalityNotEnabledError = FunctionNotImplementedError
MessageMalformedError = CodesInternalError
MessageInvalidError = CodesInternalError
MessageEndNotFoundError = CodesInternalError
MemoryAllocationError = CodesInternalError
NoDefinitionsError = CodesInternalError
NoValuesError = CodesInternalError
NullHandleError = CodesInternalError
NullPointerError = CodesInternalError
OutOfAreaError = CodesInternalError
OutOfRangeError = CodesInternalError
ReadOnlyError = CodesInternalError
RuntimeError = CodesInternalError
StringTooSmallError = CodesInternalError
ValueDifferentError = CodesInternalError
WrongArraySizeError = CodesInternalError
WrongBitmapSizeError = CodesInternalError
WrongGridError = CodesInternalError
WrongLengthError = CodesInternalError
WrongStepError = CodesInternalError
WrongStepUnitError = CodesInternalError
UnsupportedEditionError = CodesInternalError
DifferentEditionError = CodesInternalError
ArrayTooSmallError = CodesInternalError
AttributeClashError = CodesInternalError
AttributeNotFoundError = CodesInternalError
BufferTooSmallError = CodesInternalError
CodeNotFoundInTableError = CodesInternalError
ConceptNoMatchError = CodesInternalError
ConstantFieldError = CodesInternalError
FileNotFoundError = CodesInternalError
GeocalculusError = CodesInternalError
HashArrayNoMatchError = CodesInternalError
InternalArrayTooSmallError = CodesInternalError
InvalidBitsPerValueError = CodesInternalError
InvalidOrderByError = CodesInternalError
InvalidSectionNumberError = CodesInternalError
MemoryReader = object
MessageTooLargeError = CodesInternalError
MissingBufrEntryError = CodesInternalError
NoMoreInSetError = CodesInternalError
OutOfRangeError = CodesInternalError
SwitchNoMatchError = CodesInternalError
TooManyAttributesError = CodesInternalError
UnderflowError = CodesInternalError
ValueCannotBeMissingError = CodesInternalError


def _unsupported(name):
    def _fn(*args, **kwargs):
        raise FunctionNotImplementedError(
            f"{name} is not implemented yet in ecrust"
        )

    _fn.__name__ = name
    return _fn


def _noop(*args, **kwargs):
    return None


codes_any_new_from_samples = _unsupported("codes_any_new_from_samples")
codes_new_from_samples = _unsupported("codes_new_from_samples")
codes_grib_new_from_samples = _unsupported("codes_grib_new_from_samples")
codes_bufr_new_from_samples = _unsupported("codes_bufr_new_from_samples")
codes_bufr_new_from_file = _unsupported("codes_bufr_new_from_file")
codes_metar_new_from_file = _unsupported("codes_metar_new_from_file")
codes_gts_new_from_file = _unsupported("codes_gts_new_from_file")
codes_bufr_extract_headers = _unsupported("codes_bufr_extract_headers")
codes_bufr_copy_data = _unsupported("codes_bufr_copy_data")
codes_bufr_key_is_coordinate = _unsupported("codes_bufr_key_is_coordinate")
codes_bufr_key_is_header = _unsupported("codes_bufr_key_is_header")
codes_bufr_keys_iterator_new = _unsupported("codes_bufr_keys_iterator_new")
codes_bufr_keys_iterator_next = _unsupported("codes_bufr_keys_iterator_next")
codes_bufr_keys_iterator_get_name = _unsupported("codes_bufr_keys_iterator_get_name")
codes_bufr_keys_iterator_rewind = _unsupported("codes_bufr_keys_iterator_rewind")
codes_bufr_keys_iterator_delete = _unsupported("codes_bufr_keys_iterator_delete")
codes_copy_namespace = _unsupported("codes_copy_namespace")
codes_context_delete = _noop
codes_context_set_logging = _noop
codes_dump = _unsupported("codes_dump")
codes_get_gaussian_latitudes = _unsupported("codes_get_gaussian_latitudes")
codes_grib_multi_append = _unsupported("codes_grib_multi_append")
codes_grib_multi_new = _unsupported("codes_grib_multi_new")
codes_grib_multi_release = _unsupported("codes_grib_multi_release")
codes_grib_multi_write = _unsupported("codes_grib_multi_write")
codes_grib_multi_support_off = _noop
codes_grib_multi_support_on = _noop
codes_grib_multi_support_reset_file = _noop
codes_gribex_mode_off = _noop
codes_gribex_mode_on = _noop
codes_gts_header = _unsupported("codes_gts_header")
codes_index_read = _unsupported("codes_index_read")
codes_index_write = _unsupported("codes_index_write")
codes_no_fail_on_wrong_length = _noop
codes_set = _unsupported("codes_set")
codes_set_array = _unsupported("codes_set_array")
codes_set_data_quality_checks = _noop
codes_set_double = _unsupported("codes_set_double")
codes_set_double_array = _unsupported("codes_set_double_array")
codes_set_key_vals = _unsupported("codes_set_key_vals")
codes_set_long = _unsupported("codes_set_long")
codes_set_long_array = _unsupported("codes_set_long_array")
codes_set_missing = _unsupported("codes_set_missing")
codes_set_string = _unsupported("codes_set_string")
codes_set_string_array = _unsupported("codes_set_string_array")
codes_set_values = _unsupported("codes_set_values")
codes_bufr_multi_element_constant_arrays_off = _noop
codes_bufr_multi_element_constant_arrays_on = _noop


def CODES_CHECK(errid):
    if errid in (None, 0):
        return None
    raise CodesInternalError(f"ecCodes-style error status: {errid}")


message = ModuleType("ecrust.message")
message.Message = Message
message.GRIBMessage = GRIBMessage
message.BUFRMessage = BUFRMessage

reader = ModuleType("ecrust.reader")
reader.FileReader = FileReader
reader.StreamReader = StreamReader
reader.MemoryReader = MemoryReader

highlevel = ModuleType("ecrust.highlevel")
highlevel.Message = Message
highlevel.GRIBMessage = GRIBMessage
highlevel.BUFRMessage = BUFRMessage

_api_compat.install(globals())

del ModuleType
del _api_compat
del _noop
del _unsupported
del CodesHandle
del CodesKeysIterator
del CodesGribIterator
del CodesNearest
del CodesIndex
del InvalidGribIteratorError
