package org.opencb.hpg.bigdata.core.utils;

import org.opencb.commons.run.ParallelTaskRunner;

import java.util.List;
import java.util.function.Predicate;

/**
 * Created by jtarraga on 13/10/16.
 */
public class FilterTask<T> implements ParallelTaskRunner.Task<T, T> {

    protected List<List<Predicate<T>>> filters;

    public FilterTask(List<List<Predicate<T>>> filters) {
        this.filters = filters;
    }

    @Override
    public List<T> apply(List<T> batch) throws RuntimeException {
        return batch;
    }
}
