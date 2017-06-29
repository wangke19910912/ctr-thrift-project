package com.codelab.util;

import java.util.ArrayList;

/**
 * Created by vincent on 2016/12/7.
 */
public class QieCiUtils {
    private static String regex = "^[a-z0-9A-Z]+$";
    private static String regex2 = "^[,.?!%^&*(){}\\[\\]]";
    private static String regex_kongbai = "^[\\t\\n\\f\\r ]";

    public static boolean isChinesePunctuation(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.GENERAL_PUNCTUATION || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_FORMS
                ) {
            return true;
        } else {
            return false;
        }
    }

    public static ArrayList<String> qieci(String keyword) {

        ArrayList<String> words = new ArrayList<String>();

        String word = "";
        for (int i = 0; i < keyword.length(); i++) {
            String s = "" + keyword.charAt(i);
            if (isChinesePunctuation(keyword.charAt(i)) || s.matches(regex2) || s.matches(regex_kongbai)) {

                if (word != "") {
                    words.add(word);
                }
                word = "";

            } else if (s.matches(regex)) {

                word += s;

            } else {
                if (word != "") {
                    words.add(word);
                }
                word = "";
                words.add(s);
            }
        }

        return words;
    }

}
