package kr.co.mz.pmspsa;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CorpASync implements Job {

    private static final String ORI_DRIVER = "com.amazon.redshift.jdbc42.Driver";
    private static final String ORI_URL = "jdbc:redshift://bumblebee.ciof5aovn38i.ap-northeast-2.redshift.amazonaws.com:5439/prd?" +
            "AccessKeyID=AKIASCNEQF2LHJRDQMG2" +
            "&SecretAccessKey=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" +
            "&DbUser=pmspsa" +
            "&ssl=true" +
            "&tcpKeepAlive=true";
    private static final String ORI_USER = "pmspsa";
    private static final String ORI_PASSWORD = "Megazone2022@#";

    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://pmspsa-mysql-db.cmeoxjdrt43c.ap-northeast-2.rds.amazonaws.com:3306/pmspsadb?characterEncoding=UTF-8&serverTimezone=Asia/Seoul";
    private static final String USER = "admin";
    private static final String PASSWORD = "mzc1234!";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdfSec = new SimpleDateFormat("ss");
        long startDate = System.currentTimeMillis();
        System.out.println("그룹웨어 계정 동기화 시작 [시작시간 : " + sdf.format(startDate) + "]");
// ---------------------------------------------------------------------------------------------------------------------
        try {
            Class.forName(ORI_DRIVER);
            Class.forName(DRIVER);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        // 그룹웨어 Dept 정보 조회
        List<Map<String, Object>> oriDeptList = getOriDept();
        // PMS & PSA Dept DB 적재
        int insDeptCnt = insDept(oriDeptList);

        // 그룹웨어 Emp 정보 조회
        List<Map<String, Object>> oriEmpList = getOriEmp();
        // PMS & PSA Emp DB 적재
        int insEmpCnt = insEmp(oriEmpList);
// ---------------------------------------------------------------------------------------------------------------------
        long endDate = System.currentTimeMillis();
        System.out.println("그룹웨어 계정 동기화 종료 [종료시간 : " + sdf.format(endDate) + "][소요시간 : "+ sdfSec.format(endDate-startDate) +"초][부서정보 "+ insDeptCnt +"건][사원정보 " + insEmpCnt +"건]");
    }

    /**
     * 그룹웨어 부서정보 조회
     * @return
     */
    private List<Map<String, Object>> getOriDept() {
        List<Map<String, Object>> oriDeptList = new ArrayList<>();
        Connection oriCon = null;
        PreparedStatement oriPstmt = null;
        ResultSet oriRs = null;
        String oriSql = "select " +
                        "   company_code, " +
                        "   company_name, " +
                        "   dept_code, " +
                        "   dept_name, " +
                        "   parent_dept_code, " +
                        "   dept_hierarchy, " +
                        "   use_yn, " +
                        "   display_yn, " +
                        "   dept_manager, " +
                        "   detail_addr_m, " +
                        "   create_date, " +
                        "   modify_date " +
                        "from " +
                        "   bomd2.a10_gw_comp_dept ";

        try {
            oriCon = DriverManager.getConnection(ORI_URL, ORI_USER, ORI_PASSWORD);
            oriPstmt = oriCon.prepareStatement(oriSql);
            oriRs = oriPstmt.executeQuery();

            // 그룹웨어 dept 정보 조회 후 List 적재
            while (oriRs.next()) {
                Map<String, Object> oriDeptMap = new HashMap<>();
                oriDeptMap.put("company_code", oriRs.getObject("company_code"));
                oriDeptMap.put("company_name", oriRs.getObject("company_name"));
                oriDeptMap.put("dept_code", oriRs.getObject("dept_code"));
                oriDeptMap.put("dept_name", oriRs.getObject("dept_name"));
                oriDeptMap.put("parent_dept_code", oriRs.getObject("parent_dept_code"));
                oriDeptMap.put("dept_hierarchy", oriRs.getObject("dept_hierarchy"));
                oriDeptMap.put("use_yn", oriRs.getObject("use_yn"));
                oriDeptMap.put("display_yn", oriRs.getObject("display_yn"));
                oriDeptMap.put("dept_manager", oriRs.getObject("dept_manager"));
                oriDeptMap.put("detail_addr_m", oriRs.getObject("detail_addr_m"));
                oriDeptMap.put("create_date", oriRs.getObject("create_date"));
                oriDeptMap.put("modify_date", oriRs.getObject("modify_date"));
                oriDeptList.add(oriDeptMap);
            }

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            try {oriRs.close();} catch (SQLException e) {e.printStackTrace();}
            try {oriPstmt.close();} catch (SQLException e) {e.printStackTrace();}
            try {oriCon.close();} catch (SQLException e) {e.printStackTrace();}
        }

        return oriDeptList;
    }

    /**
     * 그룹웨어 사원정보 조회
     * @return
     */
    private List<Map<String, Object>> getOriEmp() {
        List<Map<String, Object>> oriEmpList = new ArrayList<>();
        Connection oriCon = null;
        PreparedStatement oriPstmt = null;
        ResultSet oriRs = null;
        String oriSql = "select  " +
                        "  email_addr,  " +
                        "  emp_seq,  " +
                        "  emp_name_kr,  " +
                        "  join_day,  " +
                        "  resign_day,  " +
                        "  check_work_yn,  " +
                        "  comp_seq,  " +
                        "  create_date,  " +
                        "  dept_seq,  " +
                        "  erp_comp_seq,  " +
                        "  erp_emp_seq,  " +
                        "  join_day_comp,  " +
                        "  resign_day_comp,  " +
                        "  use_yn,  " +
                        "  work_status,  " +
                        "  job_code  " +
                        "from  " +
                        "  bomd2.a10_gw_comp_emp  ";

        try {
            oriCon = DriverManager.getConnection(ORI_URL, ORI_USER, ORI_PASSWORD);
            oriPstmt = oriCon.prepareStatement(oriSql);
            oriRs = oriPstmt.executeQuery();

            // 그룹웨어 dept 정보 조회 후 List 적재
            while (oriRs.next()) {
                Map<String, Object> oriEmpMap = new HashMap<>();
                oriEmpMap.put("email_addr", oriRs.getObject("email_addr"));
                oriEmpMap.put("emp_seq", oriRs.getObject("emp_seq"));
                oriEmpMap.put("emp_name_kr", oriRs.getObject("emp_name_kr"));
                oriEmpMap.put("join_day", oriRs.getObject("join_day"));
                oriEmpMap.put("resign_day", oriRs.getObject("resign_day"));
                oriEmpMap.put("check_work_yn", oriRs.getObject("check_work_yn"));
                oriEmpMap.put("comp_seq", oriRs.getObject("comp_seq"));
                oriEmpMap.put("create_date", oriRs.getObject("create_date"));
                oriEmpMap.put("dept_seq", oriRs.getObject("dept_seq"));
                oriEmpMap.put("erp_comp_seq", oriRs.getObject("erp_comp_seq"));
                oriEmpMap.put("erp_emp_seq", oriRs.getObject("erp_emp_seq"));
                oriEmpMap.put("join_day_comp", oriRs.getObject("join_day_comp"));
                oriEmpMap.put("resign_day_comp", oriRs.getObject("resign_day_comp"));
                oriEmpMap.put("use_yn", oriRs.getObject("use_yn"));
                oriEmpMap.put("work_status", oriRs.getObject("work_status"));
                oriEmpMap.put("job_code", oriRs.getObject("job_code"));
                oriEmpList.add(oriEmpMap);
            }

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            try {oriRs.close();} catch (SQLException e) {e.printStackTrace();}
            try {oriPstmt.close();} catch (SQLException e) {e.printStackTrace();}
            try {oriCon.close();} catch (SQLException e) {e.printStackTrace();}
        }

        return oriEmpList;
    }

    /**
     * PMS & PSA 부서정보 TRUNCATE / INSERT
     * @param oriDeptList
     * @return
     */
    private int insDept(List<Map<String, Object>> oriDeptList) {
        int insCnt = 0;
        Connection con = null;
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO CM_SY_DEPT (CO_CD, DEPT_CD, PA_DEPT_CD, DEPT_NM, DEPT_LVL, SORT_SN, USE_YN) VALUES";

        int i = 0;
        for(Map dept : oriDeptList) {
            sql += "(";
            sql += "'"+ dept.get("company_code") +"',";
            sql += "'"+ dept.get("dept_code") +"',";
            sql += "'"+ dept.get("parent_dept_code") +"',";
            sql += "'"+ dept.get("dept_name") +"',";
            sql += 1 +",";
            sql += 1 +",";
            sql += "'"+ dept.get("use_yn") +"'";
            sql += ")";

            if(i++ < oriDeptList.size()) {
                sql += ",";
            }
        }

        try {
            con = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("pmspsa.CM_SY_USER TRUNCATE 시작 >>>>>>");
            pstmt = con.prepareStatement("TRUNCATE CM_SY_DEPT");
            pstmt.executeUpdate();
            System.out.println("pmspsa.CM_SY_USER TRUNCATE 완료 >>>>>>");
            System.out.println("pmspsa.CM_SY_USER INSERT 시작 >>>>>>");
            pstmt = con.prepareStatement(sql);
            insCnt = pstmt.executeUpdate();
            System.out.println("pmspsa.CM_SY_USER INSERT 완료 >>>>>>");


        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            try {pstmt.close();} catch (SQLException e) {e.printStackTrace();}
            try {con.close();} catch (SQLException e) {e.printStackTrace();}
        }

        return insCnt;
    }

    /**
     * PMS & PSA 사원정보 TRUNCATE / INSERT
     * @param oriEmpList
     * @return
     */
    private int insEmp(List<Map<String, Object>> oriEmpList) {
        int insCnt = 0;
        Connection con = null;
        PreparedStatement pstmt = null;
        String sql = "select '1' from dual";

        try {
            con = DriverManager.getConnection(URL, USER, PASSWORD);
            pstmt = con.prepareStatement(sql);


        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            try {pstmt.close();} catch (SQLException e) {e.printStackTrace();}
            try {con.close();} catch (SQLException e) {e.printStackTrace();}
        }

        return insCnt;
    }
}
