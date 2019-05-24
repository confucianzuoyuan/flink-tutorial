case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)

case class OrderEvent(orderId: String, eventType: String, eventTime: String)

case class PayEvent(orderId: String, eventType: String, eventTime: String)

case class OrderTimeoutEvent(orderId: String, eventType: String)

case class ApacheLogEvent(ip: String, userId: String, eventTime: String, method: String, url: String)

case class IpViewCount(ip: String, windowEnd: Long, count: Long)