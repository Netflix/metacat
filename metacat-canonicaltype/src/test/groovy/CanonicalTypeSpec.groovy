import com.netflix.metacat.canonical.type.Foo;
import spock.lang.Specification
import spock.lang.Unroll


/**
 * Created by zhenli on 12/20/16.
 */
class CanonicalTypeSpec  extends Specification {
    @Unroll
    def 'test foo'(String address) {
        def foo = new Foo()
        foo.setAddress(address)
        foo.getAddress() == 'abc'
        where:
        address << 'abc'

    }
}
