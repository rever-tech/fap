package schemamanager.domain

import com.twitter.util.FuturePool

/**
 * @author sonpn
 */
object Implicits {
  implicit def futurePool = FuturePool.unboundedPool
}
