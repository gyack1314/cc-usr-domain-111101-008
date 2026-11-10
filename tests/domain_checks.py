from datapack.domain import utc,contains,known_at
def check(case):
    case.assertEqual(utc('2026-11-01T08:00:00+08:00'),utc('2026-11-01T00:00:00Z'))
    case.assertFalse(contains('2026-01-01T00:00:00Z','2026-02-01T00:00:00Z','2026-02-01T00:00:00Z'))
    case.assertTrue(contains('2026-01-01T00:00:00Z',None,'2030-01-01T00:00:00Z'))
    with case.assertRaises(ValueError):utc('2026-01-01T00:00:00')
