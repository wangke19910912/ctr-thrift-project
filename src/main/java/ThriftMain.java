import com.codelab.AdPredictServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftMain.class);
    public static void main(String[] args) throws InterruptedException {
        LOGGER.info("MiuiAdPrediction swift: service start...");
        Thread server = new Thread(new Runnable() {
            @Override
            public void run() {
                final AdPredictServiceImpl scribeImpl = new iAdPredictServiceImpl();

                EnvironmentType environmentType = ZKFacade.getZKSettings().getEnvironmentType();

                ZKFacade.getZKSettings().setEnviromentType(environmentType);

                SwiftServiceStartupConfig startConfig = new SwiftServiceStartupConfig(null, null, false, true, true);

                SwiftServiceRunner.IThriftInstanceProvider<AdPredictServiceImpl> provider = new SwiftServiceRunner.IThriftInstanceProvider<MiuiAdPredictServiceImpl>() {
                    @Override
                    public MiuiAdPredictServiceImpl getInstance(Class<MiuiAdPredictServiceImpl> serviceImplClass) throws Exception {
                        return scribeImpl;
                    }
                };
                SwiftServiceRunner.startThriftServer(startConfig, MiuiAdPredictServiceImpl.class, provider,
                        "com.ft.ThriftMain.port", "com.ad.swift.ThriftMain.pool");
                LOGGER.info("dPrediction swift: starting thrift service.");
                System.out.println("Thrift service has started!");
            }
        });
        server.start();
        server.join();

        LOGGER.info("MiuiAdPrediction swift: service end!");
    }
}