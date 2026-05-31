package in.gov.uidai.dp.velocity.engine;

import in.gov.uidai.dp.velocity.engine.pipeline.AuthDemoPipeline;

public class AuthDemoDriver {
    public static void main(String[] args) throws Exception {
        AuthDemoPipeline pipeline = new AuthDemoPipeline();
        pipeline.buildAndExecute();
    }
}