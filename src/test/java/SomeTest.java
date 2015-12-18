import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wanghaixing
 * on 2015/12/17 15:14.
 */
public class SomeTest {
    public static void main(String[] args) {
        SimpleDateFormat dayfmt = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        System.out.println(dayfmt.format(date));

    }
}
