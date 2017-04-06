package sexy.kome.hadoop.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * Created by ZXL on 2017/4/6.
 */
public class SimpleRunner implements Tool {
    private static Configuration CONFIGURATION = new Configuration();

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {
        if (configuration != null) {
            CONFIGURATION = configuration;
        }
    }

    @Override
    public Configuration getConf() {
        return CONFIGURATION;
    }
}
