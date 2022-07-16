package realtime.util

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * 实现对象属性拷贝
 *
 * @author Akaza
 */

object MyBeanUtils {

  // 将 srcObj 中的属性值拷贝到 destObj 对应的属性上
  def copyProperties(srcObj: AnyRef, destObj: AnyRef): Unit = {
    if (srcObj == null || destObj == null) {
      return
    }
    // 获取到 srcObj 中所有的属性
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    // 处理每个属性的拷贝
    for (srcField <- srcFields) {
      Breaks.breakable {
        val getMethodName: String = srcField.getName
        val setMethodName: String = srcField.getName + "_$eq"

        // 从 srcObj 中获取 get 方法对象
        val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)

        // 从 destObj 中获取 set 方法对象
        val setMethod: Method = {
          try {
            destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
          } catch {
            // NoSuchMethodError
            case ex: Exception => Breaks.break()
          }
        }

        // 忽略 val 属性
        val destField: Field = destObj.getClass.getDeclaredField(srcField.getName)
        if (destField.getModifiers.equals(Modifier.FINAL)) {
          Breaks.break()
        }

        // 获取 srcObj 属性的值，将其赋值给 destObj 的对应属性
        setMethod.invoke(destObj, getMethod.invoke(srcObj))
      }
    }
  }
}
