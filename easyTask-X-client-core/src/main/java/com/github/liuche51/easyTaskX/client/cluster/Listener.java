package com.github.liuche51.easyTaskX.client.cluster;

public interface Listener {
    void success( String id);
    void failed(Exception e);
}
