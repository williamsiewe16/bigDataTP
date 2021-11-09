package com.opstty;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;

@Public
@Stable
public class IntTupleWritable implements WritableComparable<IntTupleWritable> {
    private int value1;
    private int value2;

    public IntTupleWritable() {
    }

    public IntTupleWritable(int value1, int value2) {
        this.set(value1, value2);
    }

    public void set(int value1, int value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public int [] get() {
        return new int[]{this.value1, this.value2};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.value1);
        dataOutput.writeInt(this.value2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value1 = dataInput.readInt();
        this.value2 = dataInput.readInt();
    }

    @Override
    public int compareTo(IntTupleWritable o) {
        int thisValue = this.value1;
        int thatValue = o.value1;
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public boolean equals(Object o) {
        if (!(o instanceof IntTupleWritable)) {
            return false;
        } else {
            IntTupleWritable other = (IntTupleWritable)o;
            return (this.value1 == other.value1) & (this.value2 == other.value2);
        }
    }
}
