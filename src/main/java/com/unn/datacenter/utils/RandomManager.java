package com.unn.datacenter.utils;

import java.util.*;

public class RandomManager
{
    private static Random randomizer_;

    // lb -> inclusive, ub -> inclusive
    public static int rand (int lb, int ub) {
        if (randomizer_ == null) {
            randomizer_ = new Random (System.currentTimeMillis ());
        }

        int dif = ub - lb + 1;
        return lb + randomizer_.nextInt (dif);
    }


    public static <T> T getOne (ArrayList<T> vals) {
        int rndIndex = rand(0, vals.size() - 1);
        return vals.get (rndIndex);
    }

    public static <T> ArrayList<T> getMany (ArrayList<T> vals, int howMany) {
        Collections.shuffle(vals);

        int retCount = Math.min(howMany, vals.size());
        ArrayList<T> output = new ArrayList<T>();

        for (int i = 0; i < retCount; ++i) {
            output.add(vals.get(i));
        }

        return output;
    }

    public static <T> List<?> getMany (T[] vals, int howMany) {
        List<?> list = Arrays.asList(vals);
        Collections.shuffle(list);
        if (howMany > list.size()) {
            return list;
        }
        return list.subList(0, howMany);
    }
}

