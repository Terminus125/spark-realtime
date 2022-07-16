package realtime.bean

case class OrderInfo(
                      id: Long =0L,
                      province_id: String=null,
                      order_status: String=null,
                      user_id: String=null,
                      total_amount:  Double=0D,
                      activity_reduce_amount: Double=0D,
                      coupon_reduce_amount: Double=0D,
                      original_total_amount: Double=0D,
                      freight_fee: Double=0D,
                      freight_fee_reduce: Double=0D,
                      expire_time: String =null,
                      refundable_time:String =null,
                      create_time: String=null,
                      operate_time: String=null,

                      // 处理其他字段得到
                      var create_date: String=null,
                      var create_hour: String=null,

                      // 查询维度表得到
                      var province_name:String=null,
                      var province_area_code:String=null,
                      var province_3166_2:String=null,
                      var province_iso_code:String=null,

                      // 查询维度表得到
                      var user_age :String=null,
                      var user_gender:String=null
                    )
