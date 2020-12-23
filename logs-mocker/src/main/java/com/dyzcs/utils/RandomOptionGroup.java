package com.dyzcs.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomOptionGroup<T> {
    private int totalWeight = 0;

    private List<RanOpt> optList = new ArrayList<>();

    public RandomOptionGroup(RanOpt<T>... opts) {
        for (RanOpt opt : opts) {
            totalWeight += opt.getWeight();
            for (int i = 0; i < opt.getWeight(); i++) {
                optList.add(opt);
            }
        }
    }

    public RanOpt<T> getRandomOpt() {
        return optList.get(new Random().nextInt(totalWeight));
    }
}
