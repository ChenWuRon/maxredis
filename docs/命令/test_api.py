import redis

def test_set_get(host='127.0.0.1', port=6380):
    """测试往服务器写入和读取数据"""
    r = redis.Redis(host=host, port=port)

    # 写入
    r.set('name', 'maxredis')
    print(f'SET name maxredis -> OK')

    # 读取
    val = r.get('name')
    assert val == b'maxredis', f'预期 maxredis，实际 {val}'
    print(f'GET name -> {val.decode()}')

    # 覆盖写
    r.set('name', 'hello')
    val = r.get('name')
    assert val == b'hello', f'预期 hello，实际 {val}'
    print(f'SET name hello -> OK, GET -> {val.decode()}')

    # 不存在的 key
    val = r.get('nonexistent')
    assert val is None, f'预期 None，实际 {val}'
    print('GET nonexistent -> (nil)')

    print('\n✅ 所有测试通过')

if __name__ == '__main__':
    test_set_get()
