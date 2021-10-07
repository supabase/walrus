import pytest
from sqlalchemy import func, select


@pytest.mark.parametrize(
    "op,type_,val_1,val_2,result",
    [
        # text
        ("eq", "text", "aaa", "aaa", True),
        ("eq", "text", "aaa", "bbb", False),
        ("neq", "text", "aaa", "aaa", False),
        ("neq", "text", "aaa", "bbb", True),
        ("lt", "text", "aaa", "bbb", True),
        ("lt", "text", "bbb", "aaa", False),
        ("lt", "text", "bbb", "bbb", False),
        ("lte", "text", "aaa", "bbb", True),
        ("lte", "text", "bbb", "aaa", False),
        ("lte", "text", "bbb", "bbb", True),
        ("gt", "text", "aaa", "bbb", False),
        ("gt", "text", "bbb", "aaa", True),
        ("gt", "text", "bbb", "bbb", False),
        ("gte", "text", "aaa", "bbb", False),
        ("gte", "text", "bbb", "aaa", True),
        ("gte", "text", "bbb", "bbb", True),
        # biginteger
        ("eq", "bigint", "1", "1", True),
        ("eq", "bigint", "2", "1", False),
        # uuid
        (
            "eq",
            "uuid",
            "639f86b1-738b-43ed-bb09-c8d3f3bafa30",
            "639f86b1-738b-43ed-bb09-c8d3f3bafa30",
            True,
        ),
        (
            "eq",
            "uuid",
            "639f86b1-738b-43ed-bb09-c8d3f3bafa30",
            "b423f213-ac24-402a-95ea-cf1d94d8e9f0",
            False,
        ),
    ],
)
def test_user_defined_eq_filter(op, type_, val_1, val_2, result, sess):
    (x,) = sess.execute(
        select(
            [
                func.cdc.check_equality_op(
                    op,
                    type_,
                    val_1,
                    val_2,
                )
            ]
        )
    ).one()
    assert x == result
