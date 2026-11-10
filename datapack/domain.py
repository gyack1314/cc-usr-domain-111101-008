from datetime import datetime,timezone
def utc(value):
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('时刻必须包含偏移')
    return result.astimezone(timezone.utc)
def contains(start,end,at):
    lo=utc(start);hi=utc(end) if end is not None else None;t=utc(at)
    if hi is not None and lo>=hi:raise ValueError('区间倒置或为空')
    return lo<=t and (hi is None or t<hi)
def known_at(row,cutoff):return contains(row['known_from'],row['known_to'],cutoff)
def valid_at(row,event_at):return contains(row['valid_from'],row['valid_to'],event_at)
