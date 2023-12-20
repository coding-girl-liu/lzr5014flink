package org.lzr;

import java.sql.Date;
import java.util.Calendar;
import org.lzr.tables.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lzr.algorithm.*;
import org.lzr.DataSource.*;

public class Main {
    private static DataStreamSource<Customer> customerSc;

    public static void main(String[] args) throws Exception{
        Date inputDate = Date.valueOf("1993-01-01");
        Calendar cal = Calendar.getInstance();
        cal.setTime(inputDate);
        cal.add(Calendar.YEAR,1);
        Date date = new Date(cal.getTimeInMillis());
        String selectedRegion = "AFRICA";
        String dataPath = "input/";

        if(args.length>0){
            for (String arg: args){
                dataPath = arg;
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CustomerSource cs = new CustomerSource(dataPath);
        NationSource ns = new NationSource(dataPath);
        LineItemSource ls = new LineItemSource(dataPath);
        OrdersSource os = new OrdersSource(dataPath);
        RegionSource rs = new RegionSource(dataPath);
        SupplierSource ss = new SupplierSource(dataPath);

        DataStreamSource<Customer> customerSc = env.addSource(cs).setParallelism(1);
        DataStreamSource<Nation> nationSc = env.addSource(ns).setParallelism(1);
        DataStreamSource<LineItem> lineitemSc = env.addSource(ls).setParallelism(1);
        DataStreamSource<Order> orderSc = env.addSource(os).setParallelism(1);
        DataStreamSource<Region> regionSc = env.addSource(rs).setParallelism(1);
        DataStreamSource<Supplier> supplierSc = env.addSource(ss).setParallelism(1);


        Long beginTime = System.currentTimeMillis();
        System.out.println("begin time is:"+beginTime);

        orderSc.filter(order -> !order.o_orderdate.before(inputDate) && order.o_orderdate.before(date))
                .connect(customerSc)
                .keyBy("o_custkey", "c_custkey")
                .process(new orderCoCust())         // Tuple3<o_custkey, o_orderkey, c_nationkey>
                .connect(nationSc)
                .keyBy(data -> data.f2, data -> data.n_nationkey)
                .process(new coNation())            // Tuple4<o_orderkey, n_nationkey, n_regionkey, n_name>
                .connect(regionSc.filter(region -> region.r_name.equals(selectedRegion)))
                .keyBy(data -> data.f2, data -> data.r_regionkey)
                .process(new coRegion())             // Tuple3<o_orderkey, r_nationkey, n_name>
                .connect(supplierSc)
                .keyBy(data -> data.f1, data -> data.s_nationkey)
                .process(new coSupplier())           // Tuple3<o_orderkey, s_suppkey, r_name>
                .connect(lineitemSc)
                .keyBy(data -> data.f0.toString() + "#" + data.f1.toString(), data -> data.l_orderkey.toString() + "#" + data.l_suppkey.toString())
                .process(new coLineItem())      //Tuple3<r_name, sum(l_extendedprice * (1 - l_discount))>
                .keyBy(data -> "key")
                .process(new sumAndOrder())
                .print();

        env.execute();

        System.out.println("Total execution time is:"+(Float.valueOf(System.currentTimeMillis()-beginTime)/1000)+"s");
    }
}

