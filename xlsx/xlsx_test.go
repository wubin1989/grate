package xlsx

import (
	"bytes"
	"io"
	"os"
	"testing"
)

// 使用testdata中的所有Excel文件测试OpenReader
func TestOpenReaderWithTestData(t *testing.T) {
	testFiles := []string{
		"../testdata/basic.xlsx",
		"../testdata/basic2.xlsx",
		"../testdata/multi_test.xlsx",
	}

	for _, filePath := range testFiles {
		t.Run(filePath, func(t *testing.T) {
			// 打开测试文件
			file, err := os.Open(filePath)
			if err != nil {
				t.Fatalf("Failed to open test file %s: %v", filePath, err)
			}
			defer file.Close()

			// 读取文件内容
			data, err := io.ReadAll(file)
			if err != nil {
				t.Fatalf("Failed to read test file %s: %v", filePath, err)
			}

			// 创建一个ReadCloser
			reader := io.NopCloser(bytes.NewReader(data))

			// 使用OpenReader打开
			source, err := OpenReader(reader)
			if err != nil {
				t.Fatalf("OpenReader failed for %s: %v", filePath, err)
			}
			defer source.Close()

			// 验证能否列出工作表
			sheets, err := source.List()
			if err != nil {
				t.Fatalf("Failed to list sheets for %s: %v", filePath, err)
			}

			if len(sheets) == 0 {
				t.Fatalf("Expected at least one sheet in %s", filePath)
			}

			// 验证能否获取每个工作表
			for _, sheetName := range sheets {
				sheet, err := source.Get(sheetName)
				if err != nil {
					t.Fatalf("Failed to get sheet %s in %s: %v", sheetName, filePath, err)
				}

				if sheet == nil {
					t.Fatalf("Sheet %s in %s is nil", sheetName, filePath)
				}

				// 测试遍历行
				rowCount := 0
				for sheet.Next() {
					rowCount++

					// 获取并检查行数据
					values := sheet.Strings()
					if values == nil {
						t.Errorf("Row %d in sheet %s of %s returned nil values",
							rowCount, sheetName, filePath)
					}

					// 获取并检查类型
					types := sheet.Types()
					if types == nil {
						t.Errorf("Row %d in sheet %s of %s returned nil types",
							rowCount, sheetName, filePath)
					}

					// 检查长度匹配
					if len(values) != len(types) {
						t.Errorf("Row %d in sheet %s of %s: values length (%d) != types length (%d)",
							rowCount, sheetName, filePath, len(values), len(types))
					}
				}

				// 检查是否有错误发生
				if err := sheet.Err(); err != nil {
					t.Errorf("Error occurred while iterating sheet %s in %s: %v",
						sheetName, filePath, err)
				}

				// 确保至少读取了一行
				if rowCount == 0 {
					t.Errorf("No rows read from sheet %s in %s", sheetName, filePath)
				}
			}
		})
	}
}
