package tw.gov.data.dataset;

import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Slf4j
@SpringBootApplication
public class Application implements CommandLineRunner {

	private static final URI DATASET_URL;

	static {
		try {
			DATASET_URL = new URI("https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=72874C55-884D-4CEA-B7D6-F60B0BE85AB0");
		} catch (URISyntaxException uriSyntaxException) {
			throw new RuntimeException(
				"無法將字符串解析為 URI❗️",
				uriSyntaxException
			);
		}
	}

	private static final File TMP_DIR = new File(
		System.getProperty("java.io.tmpdir")
	);

	public static void main(String[] args) {
		SpringApplication.run(
			Application.class,
			args
		);
	}

	/**
	 * @param httpClient HTTP 客戶端
	 * @return 下載的檔案
	 */
	private File download(final CloseableHttpClient httpClient) {
		try {
			return httpClient.execute(
				new HttpGet(DATASET_URL),
				response -> {
					// 響應狀態
					int statusCode = response.getCode();
					if (HttpStatus.SC_OK != statusCode) {
						throw new IOException(
							String.format(
								"下載失敗：%d❗️",
								statusCode
							)
						);
					}

					// 取得檔名
					String filename = null;
					Header header = response.getFirstHeader(HttpHeaders.CONTENT_DISPOSITION);
					if (null != header) {
						String value = header.getValue();
						// 解析 filename="xxx" 或 filename*=UTF-8''xxx
						if (value.contains("filename=")) {
							filename = value
								.substring(
									value.indexOf("filename=") + 9
								)
								.replaceAll("[\"']", "")
								.split(";")[0];
						}
					}
					final File file = new File(
						TMP_DIR,
						null != filename ? filename : String.format(
							"dataset#7442@%d.zip",
							System.currentTimeMillis()
						)
					);

					// 下載檔案
					final Path path = file.toPath();
					try (InputStream inputStream = response.getEntity().getContent(); OutputStream outputStream = Files.newOutputStream(path)) {
						byte[] buffer = new byte[8192];
						int bytesRead;
						long total = 0;

						while ((bytesRead = inputStream.read(buffer)) != -1) {
							outputStream.write(
								buffer,
								0,
								bytesRead
							);
							total += bytesRead;

							// 每 1MB 記錄一次進度
							if (total % (1024 * 1024) == 0) {
								log.info(
									"已下載: {} MB",
									total / (1024 * 1024)
								);
							}
						}
					} catch (IOException ioException) {
						throw new RuntimeException(
							String.format(
								"請求失敗：%s❗️",
								ioException.getMessage()
							),
							ioException
						);
					}
					return file;
				}
			);
		} catch (IOException ioException) {
			throw new RuntimeException(
				String.format(
					"下載請求失敗：%s❗️",
					ioException.getMessage()
				),
				ioException
			);
		}
	}

	/**
	 * @param httpClient HTTP 客戶端
	 * @return 元數據
	 */
	private DatasetMetaData fetchMetaData(final CloseableHttpClient httpClient) {
		try {
			return httpClient.execute(
				new HttpHead(DATASET_URL),
				response -> {
					int statusCode = response.getCode();
					if (HttpStatus.SC_OK != statusCode) {
						throw new IOException(
							String.format(
								"HEAD 請求失敗：%d❗️",
								statusCode
							)
						);
					}
					Header headerContentLength = response.getFirstHeader(HttpHeaders.CONTENT_LENGTH);
					Header headerLastModified = response.getFirstHeader(HttpHeaders.LAST_MODIFIED);
					Header headerETag = response.getFirstHeader(HttpHeaders.ETAG);
					return new DatasetMetaData(
						null != headerContentLength ? headerContentLength.getValue() : null,
						null != headerLastModified ? headerLastModified.getValue() : null,
						null != headerETag ? headerETag.getValue() : null
					);
				}
			);
		} catch (IOException ioException) {
			throw new RuntimeException(
				String.format(
					"HEAD 請求失敗：%s❗️",
					ioException.getMessage()
				),
				ioException
			);
		}
	}

	@Override
	public void run(String... args) throws Exception {
		final DatasetMetaData currentMetaData = new DatasetMetaData(
			System.getenv("CONTENT_LENGTH"),
			System.getenv("LAST_MODIFIED"),
			System.getenv("ETAG")
		);

		final File file;
		try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
			DatasetMetaData remoteMetaData = fetchMetaData(httpClient);
			if (currentMetaData.matches(remoteMetaData)) {
				log.info("無需下載、更新…");
				return;
			}

			file = download(httpClient);
		} catch (IOException ioException) {
			throw new RuntimeException(
				String.format(
					"下載失敗：%s❗️",
					ioException.getMessage()
				),
				ioException
			);
		}

		String fileName = null;
		final Path destination = TMP_DIR.toPath();
//		ZipInputStream zipInputStream = new ZipInputStream(
//			Files.newInputStream(
//				file.toPath(),
//				StandardOpenOption.READ
//			)
//		);
//		ZipEntry zipEntry = zipInputStream.getNextEntry();
//		while (zipEntry != null) {
//			String zipEntryName = zipEntry.getName();
//			if (!zipEntry.isDirectory()) {
//				Files.createDirectories(destination);
//				Files.copy(
//					zipInputStream,
//					TMP_DIR
//						.toPath()
//						.resolve(zipEntryName),
//					StandardCopyOption.REPLACE_EXISTING
//				);
//				if (zipEntryName.endsWith(".shp")) {
//					fileName = zipEntryName.replaceAll(
//						"\\.shp$",
//						""
//					);
//				}
//			}
//			zipInputStream.closeEntry();
//			zipEntry = zipInputStream.getNextEntry();
//		}
		ZipFile zipFile = new ZipFile(
			file,
			Charset.forName("Big5")
		);
		Enumeration<? extends ZipEntry> entries = zipFile.entries();
		while (entries.hasMoreElements()) {
			ZipEntry zipEntry = entries.nextElement();
			String zipEntryName = zipEntry.getName();
			Files.copy(
				zipFile.getInputStream(zipEntry),
				TMP_DIR
					.toPath()
					.resolve(zipEntryName),
				StandardCopyOption.REPLACE_EXISTING
			);
			if (zipEntryName.endsWith(".shp")) {
				fileName = zipEntryName.replaceAll(
					"\\.shp$",
					""
				);
			}
		}
		if (fileName == null) {
			throw new RuntimeException("無合規的 .shp 檔案❗️");
		}

		Arrays
			.stream(Objects.requireNonNull(TMP_DIR.listFiles()))
			.forEach(System.out::println);
	}

	/**
	 * 元數據
	 */
	record DatasetMetaData(
		String contentLength,
		String lastModified,
		String etag
	) {
		boolean matches(DatasetMetaData other) {
			return this.contentLength.equals(other.contentLength) && this.lastModified.equals(other.lastModified) && this.etag.equals(other.etag);
		}
	}
}
