package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LoginPageRenderingIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void loginPageRendersWithoutTemplateParsingFailure() {
        ResponseEntity<String> response =
                restTemplate.getForEntity("http://localhost:" + port + "/login", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .isNotNull()
                .contains("StreamCraft")
                .contains("action=\"/login\"")
                .contains("name=\"username\"");
    }

    @Test
    void loginPageRendersEnglishCopyWhenLangParameterIsSelected() {
        ResponseEntity<String> response =
                restTemplate.getForEntity("http://localhost:" + port + "/login?lang=en-US", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .isNotNull()
                .contains("lang=\"en-US\"")
                .contains("<option value=\"zh-CN\">\u7b80\u4f53\u4e2d\u6587</option>")
                .contains("<option value=\"en-US\" selected=\"selected\">English</option>")
                .contains("Welcome back")
                .contains("Sign in to your StreamCraft workspace")
                .contains("Sign in");
    }

    @Test
    void loginPageRendersChineseCopyWhenLangParameterIsSelected() {
        ResponseEntity<String> response =
                restTemplate.getForEntity("http://localhost:" + port + "/login?lang=zh-CN", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .isNotNull()
                .contains("lang=\"zh-CN\"")
                .contains("<option value=\"zh-CN\" selected=\"selected\">\u7b80\u4f53\u4e2d\u6587</option>")
                .contains("\u6b22\u8fce\u56de\u6765")
                .contains("\u8bf7\u767b\u5f55\u60a8\u7684 StreamCraft \u5de5\u4f5c\u53f0")
                .contains("\u767b\u5f55");
    }

    @Test
    void publicBrandAssetsAreServedWithoutAuthenticationRedirect() {
        ResponseEntity<byte[]> faviconResponse =
                restTemplate.getForEntity("http://localhost:" + port + "/favicon.ico", byte[].class);
        ResponseEntity<byte[]> brandLogoResponse =
                restTemplate.getForEntity("http://localhost:" + port + "/brand-logo.png", byte[].class);

        assertThat(faviconResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(faviconResponse.getHeaders().getContentType()).isEqualTo(MediaType.valueOf("image/x-icon"));
        assertThat(faviconResponse.getBody()).isNotNull().isNotEmpty();

        assertThat(brandLogoResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(brandLogoResponse.getHeaders().getContentType()).isEqualTo(MediaType.IMAGE_PNG);
        assertThat(brandLogoResponse.getBody()).isNotNull().isNotEmpty();
    }
}
