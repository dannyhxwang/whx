package com.dsk.utils;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * Created by wanghaixing on 2015/11/12.
 */
public class StringOperator {

    public static String encryptByMd5(String src) {
        HashCode hashCode = Hashing.md5().hashString(src.trim(), Charsets.UTF_8);
        return hashCode.toString();
    }

}
