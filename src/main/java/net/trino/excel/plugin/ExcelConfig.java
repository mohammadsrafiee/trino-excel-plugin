package net.trino.excel.plugin;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Configuration class for the Excel plugin in Trino. This class defines and validates plugin-specific configuration
 * parameters such as the URL from which a ZIP file containing Excel files can be fetched.
 */
public class ExcelConfig {

    private URL excelZipUrl;

    /**
     * Returns the configured URL pointing to the ZIP archive containing Excel files.
     *
     * @return a non-null {@link URL} representing the ZIP file location
     */
    @NotNull
    public URL getExcelZipUrl() {
        return excelZipUrl;
    }

    /**
     * Sets the URL for the ZIP archive containing Excel files. The string is parsed into a {@link URI} and then
     * converted into a {@link URL}. Invalid formats will result in an {@link IllegalArgumentException}. This method is
     * annotated with {@link Config} so that it can be populated via Trino's configuration framework using the key
     * {@code excel.zip.url}.
     *
     * @param excelZipUrlString the string representation of the URL
     * @return this {@code ExcelConfig} instance for method chaining
     * @throws IllegalArgumentException if the input is not a valid URI or URL
     */
    @Config("excel.zip.url")
    public ExcelConfig setExcelZipUrl(String excelZipUrlString) {
        try {
            this.excelZipUrl = new URI(excelZipUrlString).toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid URL format for excel.zip.url: %s", excelZipUrlString), e);
        }
        return this;
    }
}
