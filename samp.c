
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sys/time.h>
#include <curl/curl.h>
#include <cjson/cJSON.h>

#define API_BASE_URL "http://localhost:3005"
#define IMAGE_BUFFER_SIZE 921654
#define MAX_FILENAME 256

typedef struct imgInfo
{
  int id;
  char camNo[8];
  int t_year;
  int t_mon;
  int t_mday;
  int t_hour;
  int t_min;
  int t_sec;
  int t_mill;
  char i_location[256];
} imgInfo_t;

typedef struct
{
  char *camNo;
  int year;
  int month;
  int day;
  int hour;
  int minute;
  int second;
} QueryParams;

imgInfo_t iInfo;
unsigned char *imgData_p;
unsigned char *imgData_g;

// ============================================================================
// BASE64 ENCODING
// ============================================================================

static const char base64_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

char *encode_base64(const unsigned char *data, size_t input_length)
{
  size_t output_length = 4 * ((input_length + 2) / 3);
  char *encoded = (char *)malloc(output_length + 1);
  if (!encoded)
    return NULL;

  size_t i, j;
  for (i = 0, j = 0; i < input_length;)
  {
    uint32_t octet_a = i < input_length ? data[i++] : 0;
    uint32_t octet_b = i < input_length ? data[i++] : 0;
    uint32_t octet_c = i < input_length ? data[i++] : 0;

    uint32_t triple = (octet_a << 16) + (octet_b << 8) + octet_c;

    encoded[j++] = base64_table[(triple >> 18) & 0x3F];
    encoded[j++] = base64_table[(triple >> 12) & 0x3F];
    encoded[j++] = base64_table[(triple >> 6) & 0x3F];
    encoded[j++] = base64_table[triple & 0x3F];
  }

  size_t mod = input_length % 3;
  if (mod == 1)
  {
    encoded[output_length - 1] = '=';
    encoded[output_length - 2] = '=';
  }
  else if (mod == 2)
  {
    encoded[output_length - 1] = '=';
  }

  encoded[output_length] = '\0';
  return encoded;
}

// ============================================================================
// CURL UTILITIES
// ============================================================================

typedef struct
{
  char *data;
  size_t size;
} HttpResponse;

static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
  size_t realsize = size * nmemb;
  HttpResponse *resp = (HttpResponse *)userp;

  char *ptr = realloc(resp->data, resp->size + realsize + 1);
  if (!ptr)
  {
    printf("ERROR: Not enough memory for response\n");
    return 0;
  }

  resp->data = ptr;
  memcpy(&(resp->data[resp->size]), contents, realsize);
  resp->size += realsize;
  resp->data[resp->size] = 0;

  return realsize;
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// Get current timestamp in milliseconds
long long get_current_timestamp_ms(void)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (long long)(tv.tv_sec) * 1000 + (long long)(tv.tv_usec) / 1000;
}

// Load BMP file from disk
unsigned char *load_bmp_file(const char *filepath, size_t *out_size)
{
  FILE *fp = fopen(filepath, "rb");
  if (!fp)
  {
    printf("ERROR: Cannot open file: %s\n", filepath);
    return NULL;
  }

  fseek(fp, 0, SEEK_END);
  long file_size = ftell(fp);
  fseek(fp, 0, SEEK_SET);

  if (file_size <= 0 || file_size > IMAGE_BUFFER_SIZE)
  {
    printf("ERROR: Invalid file size: %ld bytes\n", file_size);
    fclose(fp);
    return NULL;
  }

  unsigned char *buffer = (unsigned char *)malloc(file_size);
  if (!buffer)
  {
    printf("ERROR: Memory allocation failed\n");
    fclose(fp);
    return NULL;
  }

  size_t bytes_read = fread(buffer, 1, file_size, fp);
  fclose(fp);

  if (bytes_read != (size_t)file_size)
  {
    printf("ERROR: Failed to read entire file\n");
    free(buffer);
    return NULL;
  }

  *out_size = file_size;
  printf("Loaded %ld bytes from %s\n", file_size, filepath); // file_size is long int
  return buffer;
}

// Generate filename from timestamp (format: yyMMddhhmmss_ms.bmp)
void generate_filename(long long timestamp_ms, char *output, size_t output_size)
{
  time_t sec = timestamp_ms / 1000;
  int ms = timestamp_ms % 1000;
  struct tm *tm_info = localtime(&sec);

  snprintf(output, output_size, "%02d%02d%02d%02d%02d%02d_%03d.bmp",
           tm_info->tm_year % 100,
           tm_info->tm_mon + 1,
           tm_info->tm_mday,
           tm_info->tm_hour,
           tm_info->tm_min,
           tm_info->tm_sec,
           ms);
}

// Convert struct tm to epoch timestamp with milliseconds
long long datetime_to_timestamp(int year, int month, int day,
                                int hour, int minute, int second, int millis)
{
  struct tm tm_time = {0};
  tm_time.tm_year = year - 1900;
  tm_time.tm_mon = month - 1;
  tm_time.tm_mday = day;
  tm_time.tm_hour = hour;
  tm_time.tm_min = minute;
  tm_time.tm_sec = second;

  time_t epoch_sec = mktime(&tm_time);
  return (long long)epoch_sec * 1000 + millis;
}

// IMAGE DATA POST - Upload BMP file to API

int imgDataPost(imgInfo_t iInfo, unsigned char imgData_p[], size_t img_size)
{
  CURL *curl = curl_easy_init();
  if (!curl)
  {
    printf("ERROR: CURL initialization failed\n");
    return -1;
  }

  printf("Encoding image to base64... (input bytes=%zu)\n", img_size);
  fflush(stdout);
  char *base64 = encode_base64(imgData_p, img_size);
  if (!base64)
  {
    printf("ERROR: Base64 encoding failed\n");
    curl_easy_cleanup(curl);
    return -1;
  }
  size_t b64_len = strlen(base64);
  printf("Base64 encoding complete (base64 bytes=%zu)\n", b64_len);
  if (b64_len > 0)
  {
    size_t show = b64_len > 120 ? 120 : b64_len;
    printf("Base64 sample: %.*s%s\n", (int)show, base64, (b64_len > show) ? "..." : "");
  }

  long long timestamp = get_current_timestamp_ms();
  char filename[64];
  generate_filename(timestamp, filename, sizeof(filename));

  cJSON *json = cJSON_CreateObject();
  cJSON_AddStringToObject(json, "camNo", iInfo.camNo);
  cJSON_AddNumberToObject(json, "timestamp", (double)timestamp);
  cJSON_AddStringToObject(json, "filename", filename);
  cJSON_AddStringToObject(json, "imageBase64", base64);

  char *json_str = cJSON_Print(json);
  cJSON_Delete(json);
  free(base64);

  if (!json_str)
  {
    printf("ERROR: JSON creation failed\n");
    curl_easy_cleanup(curl);
    return -1;
  }

  printf("JSON payload created (%zu bytes)\n", strlen(json_str));

  char url[512];
  snprintf(url, sizeof(url), "%s/api/frames", API_BASE_URL);
  printf("Posting to: %s\n", url);

  HttpResponse response = {0};
  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");

  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_str);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)strlen(json_str));
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

  printf("Sending POST request...\n");
  CURLcode res = curl_easy_perform(curl);
  long http_code = 0;
  int result = -1;

  if (res == CURLE_OK)
  {
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    printf("HTTP Response: %ld\n", http_code);
    if (response.data)
    {
      printf("Response: %s\n", response.data);
    }
    result = (http_code == 200) ? 0 : -1;
  }
  else
  {
    printf("ERROR: HTTP request failed: %s\n", curl_easy_strerror(res));
    result = -1;
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  free(json_str);
  if (response.data)
    free(response.data);

  return result;
}

// IMAGE DATA GET - Retrieve metadata and file from API
int imgDataGet(QueryParams params, unsigned char imgData_g[])
{
  (void)imgData_g; // Suppress unused parameter warning
  CURL *curl = curl_easy_init();
  if (!curl)
  {
    printf("ERROR: CURL initialization failed\n");
    return -1;
  }

  // Build query URL with optional parameters
  char query_url[1024];
  int len = snprintf(query_url, sizeof(query_url), "%s/api/frames?camNo=%s",
                     API_BASE_URL, params.camNo);

  if (params.year > 0)
    len += snprintf(query_url + len, sizeof(query_url) - len, "&year=%d", params.year);
  if (params.month > 0)
    len += snprintf(query_url + len, sizeof(query_url) - len, "&month=%d", params.month);
  if (params.day > 0)
    len += snprintf(query_url + len, sizeof(query_url) - len, "&day=%d", params.day);
  if (params.hour >= 0)
    len += snprintf(query_url + len, sizeof(query_url) - len, "&hour=%d", params.hour);
  if (params.minute >= 0)
    len += snprintf(query_url + len, sizeof(query_url) - len, "&minute=%d", params.minute);
  if (params.second >= 0)
    len += snprintf(query_url + len, sizeof(query_url) - len, "&second=%d", params.second);

  printf("Query URL: %s\n", query_url);

  HttpResponse response = {0};
  curl_easy_setopt(curl, CURLOPT_URL, query_url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

  CURLcode res = curl_easy_perform(curl);
  long http_code = 0;

  if (res != CURLE_OK)
  {
    printf("ERROR: Query failed: %s\n", curl_easy_strerror(res));
    curl_easy_cleanup(curl);
    if (response.data)
      free(response.data);
    return -1;
  }

  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  printf("HTTP Response: %ld\n", http_code);

  if (!response.data)
  {
    printf("ERROR: No response data\n");
    curl_easy_cleanup(curl);
    return -1;
  }

  printf("Response: %s\n", response.data);

  // Parse JSON response
  cJSON *json = cJSON_Parse(response.data);
  if (!json)
  {
    printf("ERROR: Failed to parse JSON response\n");
    free(response.data);
    curl_easy_cleanup(curl);
    return -1;
  }

  cJSON *frames = cJSON_GetObjectItem(json, "frames");
  if (!frames || cJSON_GetArraySize(frames) == 0)
  {
    printf("ERROR: No frames found in response\n");
    cJSON_Delete(json);
    free(response.data);
    curl_easy_cleanup(curl);
    return -1;
  }

  cJSON *frame = cJSON_GetArrayItem(frames, 0);
  cJSON *location = cJSON_GetObjectItem(frame, "l_location");
  if (!location || !location->valuestring)
  {
    printf("ERROR: No file location in response\n");
    cJSON_Delete(json);
    free(response.data);
    curl_easy_cleanup(curl);
    return -1;
  }

  char filename[MAX_FILENAME];
  const char *loc_str = location->valuestring;
  const char *last_slash = strrchr(loc_str, '/');
  if (last_slash)
  {
    strcpy(filename, last_slash + 1);
  }
  else
  {
    strcpy(filename, loc_str);
  }

  printf("Found filename: %s\n", filename);

  cJSON_Delete(json);
  free(response.data);

  return 0;
}

// DOWNLOAD FILE FUNCTION
int download_frame_file(const char *filename, const char *output_path)
{
  CURL *curl = curl_easy_init();
  if (!curl)
    return -1;

  char url[1024];
  snprintf(url, sizeof(url), "%s/api/frame-file?filename=%s", API_BASE_URL, filename);

  printf("Downloading: %s\n", url);

  FILE *fp = fopen(output_path, "wb");
  if (!fp)
  {
    printf("ERROR: Cannot open file for writing: %s\n", output_path);
    curl_easy_cleanup(curl);
    return -1;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

  CURLcode res = curl_easy_perform(curl);
  fclose(fp);

  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

  if (res == CURLE_OK && http_code == 200)
  {
    printf(" File downloaded: %s\n", output_path);
    curl_easy_cleanup(curl);
    return 0;
  }
  else
  {
    printf("ERROR: Download failed (HTTP %ld)\n", http_code);
    curl_easy_cleanup(curl);
    return -1;
  }
}

// HELP FUNCTION
void print_help(void)
{
  printf("\n");
  printf("---------\n");
  printf("------Surveillance System - C-----\n");
  printf("------Command-Line Interface-----\n");
  printf("---------\n");
  printf("\n");
  printf("USAGE:\n");
  printf("------\n");
  printf("\n");
  printf("1. POST - Upload BMP frame to API\n");
  printf("   ./samp.exe --post --file <filepath> --camera <camera_name>\n");
  printf("   Example: ./samp.exe --post --file test/image.bmp --camera CAM0\n");
  printf("\n");
  printf("2. GET - Retrieve frames (with optional filters)\n");
  printf("   ./samp.exe --get --camera <camera_name> [--year Y] [--month M] [--day D] [--hour H] [--minute MIN] [--second S]\n");
  printf("   Examples:\n");
  printf("     ./samp.exe --get --camera CAM0                          (all frames)\n");
  printf("     ./samp.exe --get --camera CAM0 --year 2025             (specific year)\n");
  printf("     ./samp.exe --get --camera CAM0 --year 2025 --month 11 (specific month)\n");
  printf("     ./samp.exe --get --camera CAM0 --day 10                (specific day)\n");
  printf("     ./samp.exe --get --camera CAM0 --hour 12               (specific hour)\n");
  printf("\n");
  printf("3. DOWNLOAD - Download file by filename\n");
  printf("   ./samp.exe --download --filename <filename> [--output <output_path>]\n");
  printf("   Example: ./samp.exe --download --filename 251110123456_123.bmp\n");
  printf("   Example: ./samp.exe --download --filename 251110123456_123.bmp --output myfile.bmp\n");
  printf("\n");
  printf("4. HELP - Show this message\n");
  printf("   ./samp.exe --help\n");
  printf("\n");
  printf("NOTES:\n");
  printf("------\n");
  printf("- Camera name is required for --post and --get\n");
  printf("- File path is required for --post\n");
  printf("- For --get: all filter parameters are optional. If none provided, returns all frames\n");
  printf("- Downloaded files are saved as 'downloaded_frame.bmp' by default\n");
  printf("- API server must be running on http://localhost:3005\n");
  printf("\n");
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char *argv[])
{
  printf("DEBUG: main() started, argc=%d\n", argc);
  fflush(stdout);

  if (argc < 2)
  {
    print_help();
    return 1;
  }

  printf("DEBUG: argc >= 2, continuing\n");
  fflush(stdout);

  // Initialize CURL
  curl_global_init(CURL_GLOBAL_DEFAULT);

  printf("DEBUG: Starting buffer allocation\n");
  fflush(stdout);

  // Allocate buffers
  imgData_p = (unsigned char *)malloc(IMAGE_BUFFER_SIZE);
  printf("DEBUG: imgData_p allocated: %p\n", (void *)imgData_p);
  fflush(stdout);

  imgData_g = (unsigned char *)malloc(IMAGE_BUFFER_SIZE);
  printf("DEBUG: imgData_g allocated: %p\n", (void *)imgData_g);
  fflush(stdout);

  if (!imgData_p || !imgData_g)
  {
    printf("ERROR: Memory allocation failed\n");
    curl_global_cleanup();
    return -1;
  }

  printf("DEBUG: Buffer allocation complete\n");
  fflush(stdout);

  int result = -1;

  // Parse --help
  if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0)
  {
    print_help();
    result = 0;
  }
  // Parse --post
  else if (strcmp(argv[1], "--post") == 0)
  {
    char *filepath = NULL;
    char *camera = NULL;

    // Parse arguments
    for (int i = 2; i < argc; i++)
    {
      if (strcmp(argv[i], "--file") == 0 && i + 1 < argc)
      {
        filepath = argv[++i];
      }
      else if (strcmp(argv[i], "--camera") == 0 && i + 1 < argc)
      {
        camera = argv[++i];
      }
    }

    if (!filepath || !camera)
    {
      printf("ERROR: --post requires --file and --camera arguments\n");
      printf("Usage: samp.exe --post --file <filepath> --camera <camera_name>\n");
      result = -1;
    }
    else
    {
      // Load BMP file
      size_t file_size;
      unsigned char *bmp_data = load_bmp_file(filepath, &file_size);
      if (!bmp_data)
      {
        result = -1;
      }
      else
      {
        printf("File loaded successfully\n");
        fflush(stdout);

        memcpy(imgData_p, bmp_data, file_size);
        free(bmp_data);
        printf("Image copied to buffer\n");
        fflush(stdout);

        // Create imgInfo_t with current timestamp
        imgInfo_t info;
        strcpy(info.camNo, camera);
        printf("Camera name set\n");
        fflush(stdout);

        // Get current date/time
        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        info.t_year = tm_info->tm_year + 1900;
        info.t_mon = tm_info->tm_mon + 1;
        info.t_mday = tm_info->tm_mday;
        info.t_hour = tm_info->tm_hour;
        info.t_min = tm_info->tm_min;
        info.t_sec = tm_info->tm_sec;

        struct timeval tv;
        gettimeofday(&tv, NULL);
        info.t_mill = (tv.tv_usec / 1000) % 1000;
        printf("Timestamp created\n");
        fflush(stdout);

        printf("Calling imgDataPost...\n");
        fflush(stdout);

        printf("DEBUG: passing img_size=%zu to imgDataPost\n", file_size);
        fflush(stdout);

        result = imgDataPost(info, imgData_p, file_size);

        printf("imgDataPost completed\n");
        fflush(stdout);

        if (result == 0)
        {
          printf("Frame successfully posted to API\n");
        }
        else
        {
          printf("Failed to post frame\n");
        }
      }
    }
  }
  // Parse --get
  else if (strcmp(argv[1], "--get") == 0)
  {
    char *camera = NULL;
    int year = 0, month = 0, day = 0;
    int hour = -1, minute = -1, second = -1;

    // Parse arguments
    for (int i = 2; i < argc; i++)
    {
      if (strcmp(argv[i], "--camera") == 0 && i + 1 < argc)
      {
        camera = argv[++i];
      }
      else if (strcmp(argv[i], "--year") == 0 && i + 1 < argc)
      {
        year = atoi(argv[++i]);
      }
      else if (strcmp(argv[i], "--month") == 0 && i + 1 < argc)
      {
        month = atoi(argv[++i]);
      }
      else if (strcmp(argv[i], "--day") == 0 && i + 1 < argc)
      {
        day = atoi(argv[++i]);
      }
      else if (strcmp(argv[i], "--hour") == 0 && i + 1 < argc)
      {
        hour = atoi(argv[++i]);
      }
      else if (strcmp(argv[i], "--minute") == 0 && i + 1 < argc)
      {
        minute = atoi(argv[++i]);
      }
      else if (strcmp(argv[i], "--second") == 0 && i + 1 < argc)
      {
        second = atoi(argv[++i]);
      }
    }

    if (!camera)
    {
      printf("ERROR: --get requires --camera argument\n");
      printf("Usage: samp.exe --get --camera <camera_name> [--year Y] [--month M] [--day D] [--hour H] [--minute MIN] [--second S]\n");
      result = -1;
    }
    else
    {
      QueryParams params;
      params.camNo = camera;
      params.year = year;
      params.month = month;
      params.day = day;
      params.hour = hour;
      params.minute = minute;
      params.second = second;

      result = imgDataGet(params, imgData_g);
      if (result == 0)
      {
        printf("Frames metadata retrieved successfully.\n");
      }
      else
      {
        printf(" Failed to retrieve frames metadata\n");
      }
    }
  }
  // Parse --download
  else if (strcmp(argv[1], "--download") == 0)
  {
    char *filename = NULL;
    char *output_path = NULL;

    // Parse arguments
    for (int i = 2; i < argc; i++)
    {
      if (strcmp(argv[i], "--filename") == 0 && i + 1 < argc)
      {
        filename = argv[++i];
      }
      else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc)
      {
        output_path = argv[++i];
      }
    }

    if (!filename)
    {
      printf("ERROR: --download requires --filename argument\n");
      printf("Usage: samp.exe --download --filename <filename> [--output <output_path>]\n");
      result = -1;
    }
    else
    {
      // If output_path is not provided, use filename as output_path
      if (!output_path)
        output_path = filename;
      result = download_frame_file(filename, output_path);
    }
  }
  else
  {
    printf("ERROR: Unknown command: %s\n", argv[1]);
    print_help();
    result = -1;
  }

  // Cleanup CURL
  curl_global_cleanup();

  // Free allocated buffers
  if (imgData_p)
    free(imgData_p);
  if (imgData_g)
    free(imgData_g);

  return result;
}
