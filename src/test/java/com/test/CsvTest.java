package com.test;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvParser;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import org.junit.Test;


public class CsvTest {
    @Test
    public void test1HutoolCsvparser() {

        try (CsvParser cp = new CsvParser(FileUtil.getBOMReader(FileUtil.file("D:\\CSV问题文件\\文件中部分多个换行换行符 - 副本\\tet.csv")), null)) {
            while (cp.hasNext()) {
                CsvRow next = cp.next();
                System.out.println(next);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


/*    @Test
    public void test2HutoolCsvReader() {

        try (  CsvUtil.getReader()) {
            while (cp.hasNext()) {
                CsvRow next = cp.next();
                System.out.println(next);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }*/
}
