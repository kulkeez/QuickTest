package com.acme.quicktest;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Custom Rejection Handler when the queue is full and no more workers can be added.
 * 
 * @author kulkarvi
 *
 */
public class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        System.out.println(r.toString() + " is rejected");
    }

}
