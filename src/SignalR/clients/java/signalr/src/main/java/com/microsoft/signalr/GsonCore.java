package com.microsoft.signalr;

import com.google.gson.Gson;

public class GsonCore {

    private static Gson instance;
    public static Gson getInstance() {
        if (instance == null)
            instance = new Gson();
        return instance;
    }
}
