'''
Configuration values and parameters for running
comparison tests with redis-py
'''

key = 'MOCKREDIS-key-{test}'
attr = 'attr-{test}'

values = [
    1,
    '1',
    '12.123123',
    -123,
    -122,
    "-1234.123",
    "some text",
    [1, 3, 5, '12312', 'help'],
    {'key':'value', 'key2':12}
]


redis_writes = dict(

    set=dict(
        params=[
            ["{key}-{index}", "{value}"],
        ],
        keys=["simple"],
        values=values
    ),
    hset=dict(
        params=[
            ["{key}", "{attr}-{index}", "{value}"],
        ],
        keys=["hashset"],
        values=values,
        attr=[str(r) for r in range(10)]
    ),
    sadd=dict(
        params=[
            ["{key}-{index}", "{value}"],
        ],
        keys=["set"],
        values=values
    )
)


redis_reads = dict(

    get=dict(
        params=[
            ["{key}-{index}"]
        ],
        keys=["simple"]  # keys to read from
    ),
    hget=dict(
        params=[
            ["{key}", "{attr}-{index}"]
        ],
        keys=["hashset"],
        attr=[str(r) for r in range(10)]
    ),
    smembers=dict(
        params=[
            ["{key}"]
        ],
        keys=["set", "empty"]
    )
)
