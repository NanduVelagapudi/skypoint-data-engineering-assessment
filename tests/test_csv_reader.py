"""parse_csv works on bytes, so these tests need no files on disk."""

from conftest import UTF8_BOM, synthetic_file

from pipeline.csv_reader import MAX_REPORTED_MALFORMED, parse_csv
from pipeline.errors import ReasonCode, format_reasons

HEADER = ["id", "name", "amount"]


def test_lf_file():
    result = parse_csv(b"id,name,amount\n1,a,10\n2,b,20\n")

    assert result.failures == []
    assert result.header == HEADER
    assert result.records == [["1", "a", "10"], ["2", "b", "20"]]
    assert result.record_count == 2


def test_crlf_file_parses_the_same_as_lf():
    lf = parse_csv(b"id,name,amount\n1,a,10\n2,b,20\n")
    crlf = parse_csv(b"id,name,amount\r\n1,a,10\r\n2,b,20\r\n")

    assert crlf == lf
    assert not any("\r" in value for record in crlf.records for value in record)


def test_utf8_bom_is_stripped_from_the_first_column_name():
    result = parse_csv(UTF8_BOM + b"id,name,amount\r\n1,a,10\r\n")

    assert result.header == HEADER
    assert result.records == [["1", "a", "10"]]


def test_quoted_commas_and_escaped_quotes():
    result = parse_csv(b'id,name,amount\n1,"Smith, John","1,250.00"\n2,"say ""hi""",5\n')

    assert result.failures == []
    assert result.records == [["1", "Smith, John", "1,250.00"], ["2", 'say "hi"', "5"]]


def test_quoted_newline_stays_inside_one_record():
    result = parse_csv(b'id,name,amount\r\n1,"line one\r\nline two",10\r\n2,b,20\r\n')

    assert result.record_count == 2
    assert result.records[0] == ["1", "line one\r\nline two", "10"]


def test_values_are_preserved_exactly():
    result = parse_csv(' id,name,amount\n 1 ,,"  $ 1,250 "\n3,"",N/A\n4,José,1.5K\n'.encode("utf-8"))

    assert result.header == [" id", "name", "amount"]
    assert result.records == [[" 1 ", "", "  $ 1,250 "], ["3", "", "N/A"], ["4", "José", "1.5K"]]


def test_header_only_file_has_zero_records():
    result = parse_csv(b"id,name,amount\n")

    assert result.failures == []
    assert result.record_count == 0


def test_last_record_without_trailing_newline_is_counted():
    result = parse_csv(b"id,name,amount\n1,a,10\n2,b,20")

    assert result.failures == []
    assert result.record_count == 2


def test_short_and_long_records_are_malformed_without_echoing_values():
    result = parse_csv(b"id,name,amount\n1,a,10\n2,Doe\n3,Roe,10,extra\n")

    assert [str(f) for f in result.failures] == [
        "MALFORMED_RECORD(record=2,fields=2,expected=3)",
        "MALFORMED_RECORD(record=3,fields=4,expected=3)",
    ]
    assert result.record_count == 3
    reasons = format_reasons(result.failures)
    assert "Doe" not in reasons and "Roe" not in reasons and "extra" not in reasons


def test_blank_line_is_a_malformed_record():
    result = parse_csv(b"id,name,amount\n1,a,10\n\n2,b,20\n")

    assert [str(f) for f in result.failures] == ["MALFORMED_RECORD(record=2,fields=0,expected=3)"]


def test_many_malformed_records_are_capped():
    content = b"id,name,amount\n" + b"bad\n" * (MAX_REPORTED_MALFORMED + 4)

    result = parse_csv(content)

    assert len(result.failures) == MAX_REPORTED_MALFORMED + 1
    assert str(result.failures[-1]) == "MALFORMED_RECORD(additional_records=4)"


def test_file_truncated_mid_record_fails():
    content = synthetic_file("EPIC_NORTH", n_rows=5).content
    truncated = content[: len(content) - 40]  # cuts into the last record

    result = parse_csv(truncated)

    assert result.record_count == 5
    assert [f.code for f in result.failures] == [ReasonCode.MALFORMED_RECORD]
    assert result.failures[0].detail.startswith("record=5,")


def test_unterminated_quote_at_end_of_file_fails():
    result = parse_csv(b'id,name,amount\n1,a,10\n2,"Doe, Ja')

    assert [str(f) for f in result.failures] == ["CSV_PARSE_ERROR(record=2)"]
    assert result.complete is False
    assert result.record_count is None
    assert "Doe" not in format_reasons(result.failures)


def test_invalid_utf8_fails():
    result = parse_csv(b"id,name,amount\n1,\xff\xfe,10\n")

    assert [str(f) for f in result.failures] == ["ENCODING_ERROR(byte_offset=17)"]
    assert result.record_count is None


def test_utf16_file_is_rejected_not_guessed():
    result = parse_csv("id,name,amount\n1,a,10\n".encode("utf-16"))

    assert [f.code for f in result.failures] == [ReasonCode.ENCODING_ERROR]


def test_empty_file_has_no_header():
    assert [f.code for f in parse_csv(b"").failures] == [ReasonCode.HEADER_MISSING]


def test_blank_first_line_has_no_header():
    result = parse_csv(b"\nid,name,amount\n1,a,10\n")

    assert [f.code for f in result.failures] == [ReasonCode.HEADER_MISSING]
    assert result.record_count is None
