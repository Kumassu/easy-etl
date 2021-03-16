package song.pan.etl.config;


import io.swagger.annotations.ApiOperation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.Tag;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger.web.*;

@Configuration
@Controller
public class Swagger2Config {


    @GetMapping
    public String doc() {
        return "redirect:/swagger-ui.html";
    }


    @Bean
    public Docket openApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(new ApiInfoBuilder()
                        .title("Easy ETL API")
                        .description("")
                        .version("1.0.0")
                        .termsOfServiceUrl(null)
                        .license(null)
                        .licenseUrl(null)
                        .build()
                )
                .groupName("OpenApi")
                .tags(new Tag("ETL", "", 1))
                .select()
                .apis(RequestHandlerSelectors.basePackage("song.pan.etl.controller"))
                .apis(RequestHandlerSelectors.withMethodAnnotation(ApiOperation.class))
                .build();
    }


    @Bean
    public UiConfiguration uiConfiguration() {
        return UiConfigurationBuilder.builder()
                .deepLinking(false)
                .displayOperationId(false)
                .defaultModelsExpandDepth(0)
                .defaultModelRendering(ModelRendering.EXAMPLE)
                .displayRequestDuration(true)
                .docExpansion(DocExpansion.LIST)
                .filter(false)
                .maxDisplayedTags(null)
                .operationsSorter(OperationsSorter.ALPHA)
                .showExtensions(false)
                .tagsSorter(TagsSorter.ALPHA)
                .validatorUrl(null)
                .build();
    }


}
