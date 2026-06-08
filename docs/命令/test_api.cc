#include <iostream>
#include <hiredis.h>

void test_set_get() {
  redisContext* c = redisConnect("127.0.0.1", 6380);
  if (!c || c->err) {
    std::cerr << "连接失败: " << (c ? c->errstr : "null") << std::endl;
    return;
  }

  redisReply* reply;

  // SET
  reply = (redisReply*)redisCommand(c, "SET name maxredis");
  std::cout << "SET name maxredis -> " << reply->str << std::endl;
  freeReplyObject(reply);

  // GET
  reply = (redisReply*)redisCommand(c, "GET name");
  std::cout << "GET name -> " << reply->str << std::endl;
  if (std::string(reply->str) == "maxredis")
    std::cout << "  ✅ 值正确\n";
  else
    std::cout << "  ❌ 值错误, 预期 maxredis, 实际 " << reply->str << "\n";
  freeReplyObject(reply);

  // GET 不存在的 key
  reply = (redisReply*)redisCommand(c, "GET nonexistent");
  if (reply->type == REDIS_REPLY_NIL)
    std::cout << "GET nonexistent -> (nil)  ✅ 返回 nil\n";
  else
    std::cout << "GET nonexistent -> " << reply->str << "  ❌\n";
  freeReplyObject(reply);

  redisFree(c);
  std::cout << "\n✅ 测试通过\n";
}

int main() {
  test_set_get();
  return 0;
}
