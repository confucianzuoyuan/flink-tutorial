package com.atguigu.refactorcode;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

class ResettableCountSource implements SourceFunction<Long>, CheckpointedFunction {

    private Boolean isRunning = true;
    private Long cnt;
    private ListState<Long> offsetState;

    @Override
    public void run(SourceContext<Long> ctx) {
        while (isRunning && cnt < Long.MAX_VALUE) {
            // synchronize data emission and checkpoints
            synchronized (ctx.getCheckpointLock()) {
                cnt += 1;
                ctx.collect(cnt);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext snapshotCtx) throws Exception {
        // remove previous cnt
        offsetState.clear();
        // add current cnt
        offsetState.add(cnt);
    }

    @Override
    public void initializeState(FunctionInitializationContext initCtx) throws Exception {

        ListStateDescriptor<Long> desc = new ListStateDescriptor<>("offset", Types.LONG());
        offsetState = initCtx.getOperatorStateStore().getListState(desc);
        // initialize cnt variable
        Iterable<Long> it = offsetState.get();
        if (null == it || !it.iterator().hasNext()) {
            cnt = -1L;
        } else {
            cnt = it.iterator().next();
        }
    }
}
