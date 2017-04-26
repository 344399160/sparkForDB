package label.common;

/**
 * 描述：异常信息
 * author qiaobin   2016/10/19 10:17.
 */
public class MessageException extends RuntimeException {
    private static final long serialVersionUID = 209248116271894410L;

    public MessageException(String message){ super(message);}

    public MessageException(Throwable e) { super(e);}

    public MessageException(String message, Throwable e) { super(message, e);}
}
