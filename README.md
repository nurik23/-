# -
plsql \ sql+

-----------------------------
pragma include(::[DEBUG_TRIGGER].[MACRO_LIB]);


type TbpZalBody is record(
	zal_id			varchar2(100),
	zal_body_id		varchar2(100),
	zal_num_id		varchar2(100)
);

type TblZalBodys is table of TbpZalBody;

vTblZalBody	TblZalBodys;

function GetZalRealSum2	(vZalBodyID_CODE  varchar2(100), vVal in out varchar2(3), vRetSumKGS in out number) return number is
	retVal	varchar2(3);
	retSum	number;
	RetSumKGS number;
	i integer;
begin
	--i := 0;
	retVal	:= null;
	retSum	:= 0;
	RetSumKGS := 0;
	debug_pipe('Функция по коду 2'||vZalBodyID_CODE, 0);
	for( select cr2(
			--zl_mon%id														:id_,
			--nvl(zl_mon.cost, a2.[SUMMA])									:cot,

			--a2.[SUMMA]														:zal_sum,
			a2.[VALUTA].[cur_short]											:val,
			a2.[SUMMA]														:zal_sum,
			nvl(a2.[N_COUNT],1)												: cnt,
			::[DOCUMENT].[LIB_CUR].get_rate(a2.[VALUTA], v_date)			:kurs,
			(select zl_log(zl_log.[COST]:cost) in ::[ZLK_M_CR_ZAL_LOG], ([ZLK_M_CRD_ZALOG] all :cr_zl) all where
				zl_log.[CRED_ZALOG]=cr_zl%id and cr_zl.[GUARANTEE]=zal%id and cr_zl.[CREDIT]=cr2%id and zl_log.[VA]=a2%id
				and nvl(zl_log.[UPDATED], sysdate)<=to_date('01/02/2020')
				and nvl(zl_log.[CREATED], sysdate)<=to_date('01/02/2020') and rownum=1)		:cot
	)IN	
		::[PR_CRED],(::[ZALOG] all :zal),(::[PART_TO_LOAN] all :part),
		 (::[ZALOG_BODY] all :a2),(::[zalog_values] all :a4),
		(::[ZBADD_HOUSE] all :zbh)

		all
		where   cr2.date_begin<=v_date
				and (nvl(cr2.date_close, sysdate)>=v_date)
				--<
				and zal.date_begin<=v_date
				and (nvl(zal.date_close, sysdate)>=v_date)
				-->
				and cr2.kind_credit.name!='Технический овердрафт'
				and cr2.com_status.name in ('Работает','Закрыт')
				and cr2%id = part.[PRODUCT]
				and part%collection = zal.[PART_TO_LOAN]
				and zal.[ZALOG_BODY]=a4%collection
				and a4.[ZALOG_BODY]=a2%id
				--and zl_mon.[VA]=a2%id
				and a2.[ADDS]=zbh%id
				and zbh.[ID_CODE]=vZalBodyID_CODE
				--and zl_mon.cost	is not null
				--and zl_mon.[cred_zalog]=crd_zal%id
				--and crd_zal.[guarantee]=zal%id
				--and crd_zal.[credit]=cr%id
				
				
		order by zal%id --desc
	)loop
		--i := i+1;
		debug_pipe(i||' Находимся в цикле не нулевых сумм', 0);
		if retVal is null  then --and cc.cot is not null then -----
			retVal	:= cr2.val;
			retSum	:= nvl(cr2.cot, cr2.zal_sum);
			RetSumKGS := nvl(cr2.cot, cr2.zal_sum)*cr2.kurs;
			debug_pipe(i||'222222) retVal= Пусто retSum := '||retSum, 0);
		elsif retVal<>'KGS' and cr2.val='KGS' then
			retVal	:= cr2.val;
			retSum	:= nvl(cr2.cot, cr2.zal_sum);
			RetSumKGS := nvl(cr2.cot, cr2.zal_sum)*cr2.kurs;
			debug_pipe(i||'22222 ) retVal= '||retVal||' retSum := '||retSum, 0);
		end if;
	end loop;
		debug_pipe(i||' 2 retSum '||retSum, 0);
vVal := nvl(retVal, vVal);
vRetSumKGS := RetSumKGS;
return retSum;
					
end;

/*
function GetZalRealSum	(vZalBodyID_CODE  varchar2(100), vVal in out varchar2(3), vRetSumKGS in out number) return number is
	retVal	varchar2(3);
	retSum	number;
	RetSumKGS number;
	i integer;
begin
	--i := 0;
	retVal	:= null;
	retSum	:= 0;
	RetSumKGS := 0;
	debug_pipe('Функция по коду '||vZalBodyID_CODE, 0);
	for( select cr(
			zl_mon%id														:id_,
			nvl(zl_mon.cost, a2.[SUMMA])									:cot,
			a2.[VALUTA].[cur_short]											:val,
			a2.[SUMMA]														:zal_sum,
			nvl(a2.[N_COUNT],1)												: cnt,
			::[DOCUMENT].[LIB_CUR].get_rate(a2.[VALUTA], v_date)			:kurs
	)IN	--::[ZLK_M_CR_ZAL_LOG],(::[ZALOG_BODY] all :a2),(::[zalog_values] all :a4), (::[zalog] all :a3),
		--(::[ZBADD_HOUSE] all :zbh), (::[PR_CRED] all : pr_cred), (::[PART_TO_LOAN] all :part) all
		::[PR_CRED],(::[ZALOG] all :zal),(::[PART_TO_LOAN] all :part),
		(::[ZLK_M_CR_ZAL_LOG] all :zl_mon), (::[ZALOG_BODY] all :a2),(::[zalog_values] all :a4),
		(::[ZBADD_HOUSE] all :zbh), (::[ZLK_M_CRD_ZALOG] all :crd_zal) all
		where   cr.date_begin<=v_date
				and (nvl(cr.date_close, sysdate)>=v_date)
				and cr.kind_credit.name!='Технический овердрафт'
				and cr.com_status.name in ('Работает','Закрыт')
				and cr%id = part.[PRODUCT]
				and part%collection = zal.[PART_TO_LOAN]
				and zal.[ZALOG_BODY]=a4%collection
				and a4.[ZALOG_BODY]=a2%id
				and zl_mon.[VA]=a2%id
				and a2.[ADDS]=zbh%id
				and zbh.[ID_CODE]=vZalBodyID_CODE
				--and zl_mon.cost	is not null
				and zl_mon.[cred_zalog]=crd_zal%id
				and crd_zal.[guarantee]=zal%id
				and crd_zal.[credit]=cr%id
				and nvl(zl_mon.[UPDATED], to_date('01/02/2020'))<=to_date('01/02/2020')
				and nvl(zl_mon.[CREATED], sysdate)<=to_date('01/02/2020')
		order by zal%id desc
	)loop
		--i := i+1;
		debug_pipe(i||' Находимся в цикле не нулевых сумм', 0);
		if retVal is null  then --and cc.cot is not null then -----
			retVal	:= cr.val;
			retSum	:= cr.cot;
			RetSumKGS := cr.cot*cr.kurs;
			debug_pipe(i||') retVal= Пусто retSum := '||retSum, 0);
		elsif retVal<>'KGS' and cr.val='KGS' then
			retVal	:= cr.val;
			retSum	:= cr.cot;
			RetSumKGS := cr.cot*cr.kurs;
			debug_pipe(i||') retVal= '||retVal||' retSum := '||retSum, 0);
		end if;
	end loop;*/
	
	/*if retVal is null then
		--debug_pipe(i||' Вот мы здесь', 0);

			for( select a2(
			a2%id															:id_,
			--cc.cost															: cot,
			a2.[VALUTA].[cur_short]											:val,
			a2.[SUMMA]														:zal_sum,
			nvl(a2.[N_COUNT],1)													: cnt,
			a2.[SUMMA_NT]													:sum_nt,
			::[DOCUMENT].[LIB_CUR].get_rate(a2.[VALUTA], v_date)			:kurs
	)IN	::[ZALOG_BODY],(::[zalog_values] all :a4), (::[zalog] all :a3),
		(::[ZBADD_HOUSE] all :zbh), (::[PR_CRED] all : pr_cred), (::[PART_TO_LOAN] all :part) all
		
		where
			a3.[PART_TO_LOAN]=part%collection
			and part.[PRODUCT]=pr_cred%id
			and pr_cred.date_begin<=v_date
			and (nvl(pr_cred.date_close, sysdate)>v_date)
			--cc.va = a2%id
			and a3.zalog_body = a4%collection
			and a4.zalog_body = a2%id
			and zbh%id=a2.[ADDS]
			--and a2.[ADDS]->(ZBADD_HOUSE)[ID_CODE]=vZalBodyID_CODE
			and zbh.[ID_CODE]=vZalBodyID_CODE
			and pr_cred.com_status.name in ('Работает','Закрыт')
			--and cc.cost is not null	
		--order by cc%id desc
	)loop
			--debug_pipe(i||' a2%id '||a2.id_, 0);
			if retVal is null then
				retVal	:= a2.val;
				retSum	:= a2.zal_sum;
				RetSumKGS := a2.sum_nt; --a2.zal_sum*a2.kurs;
			elsif retVal<>'KGS' and a2.val='KGS' then
				retVal	:= a2.val;
				retSum	:= a2.zal_sum;
				RetSumKGS :=a2.sum_nt; --a2.zal_sum*a2.kurs;
			end if;
			i :=i+1;
		end loop;
	end if;*/
/*		debug_pipe(i||' retSum '||retSum, 0);
vVal := nvl(retVal, vVal);
vRetSumKGS := RetSumKGS;
return retSum;
					
end;
*/

procedure DRAW_REPORT is
--------------------------------------------------------------------------------------------------
row number:=6;
cnt number:=0;
test1 number;
v_couter number:=0;
str string(32000);str2 string(32000); tmp string(32000);str3 string(32000);
ft_unit	::[RECONT].[UNIT]%type;
p_req   in out [REQ_CLIENT];
id_zal ref [zalog];
credit ref [PR_CRED];
prod ref [PART_TO_LOAN];
v_bs    varchar2(5);
v_id    number ;
vIdx	integer;
inn_cnt number;

CurShort	varchar2(3);
SumNat		number;
Summ	number;
ii number;

vCodeID varchar2(30);

begin
EXCEL.WRITE(1, 6, to_char(V_DATE,'DD.MM.YYYY'));
EXCEL.WRITE(1, 25, ::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CODE_ISO] = '840'), V_DATE, ft_unit));
EXCEL.WRITE(2, 25, ::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CODE_ISO] = '978'), V_DATE, ft_unit));
EXCEL.WRITE(3, 25, ::[DOCUMENT].[LIB_CUR].Get_Rate_For_Date(::[FT_MONEY]%locate(x where x.[CODE_ISO] = '643'), V_DATE, ft_unit));

vIdx := 1;
--vTblZalBody := null;

for( select cr(	
				--cr%id															:ID_,
				 cr.[CLIENT].[VIDS_CL].[CATEGORY].[NAME]                       : cotegory
				 ,zal.num_dog													: nd
				 ,zal.date_begin												: dt
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[VALUTA].[CUR_SHORT]			: fin
				 ,guar.[NAME]													: nm
				 ,nvl(zal.[ZALOG_BODY]->[ZALOG_BODY].[N_COUNT], 1)				: coun
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[SUMMA]      					: zb_sm
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[DISCONT]    					: ds
		         ,::[DOCUMENT].[LIB_CUR].get_rate(zal.[ZALOG_BODY]->[ZALOG_BODY].[VALUTA],v_date)		:kurs
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[STORAGE]						: adr
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[NAME]	    					: op
				 ,nvl(zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[ID_CODE], 'NULL') : n_id
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[USEFUL_AREA] :n_us
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[TOTAL_AREA] :n_tot
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[LIVING_AREA] :n_liv
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[LAND_AREA] :n_ar
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[ADDRESS].[IMP_STR]:n_str
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[owner].name :a_cli
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[MARKA] :a_mark
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[GOS_NUMB] :a_gos
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[YEARTS] :a_year
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[COLOR] :a_col
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[KUZ] :a_kuz
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[NUMDV] :a_dv
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[VOL] :a_vol
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[TIPTS] :a_tip
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[PTSN] :a_pt
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[DATE_PTS] :a_dt
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[ORG_TEHPAS].[NAME] :a_tech
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(Z_DEPOSIT)[DOGOVOR].[NUM_DOG] :depn
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(Z_DEPOSIT)[DOGOVOR].[account].main_usv.num :depn_bs
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(Z_DEPOSIT)[DOGOVOR].[account].main_v_id :depn_ls
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS] :zl
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[OWNER_RIGHT].[NAME] :cl
			     ,zal%id														:z_id
			     ,zal.user_zalog												:zd
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[KIND_ZAL_BODY].[CODE]			:kind_body_code
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[SAFING_COMP].[NAME]			: komp
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[SAFING_COMP]					: komp_id
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY]%id							: zb_id
			     --,cr.KIND_CREDIT.name											: k_cred
			    -- , cr%id														: cr_id
				
			
	
		)
		IN	::[PR_CRED],(::[ZALOG] all :zal),(::[PART_TO_LOAN] all :part),(::[GUARANTEES] all :guar) all
		where  (V_FILIAL IS NULL OR V_FILIAL = cr.[FILIAL])
				and (cr.kind_credit.reg_rules.short_name not in ('TECH_OVER','CRED_OVER') or cr.kind_credit.reg_rules.short_name = 'CRED_OVER')
				--< 26/02/2020 mseidesanov
				and zal.date_begin<=v_date
				and (nvl(zal.date_close, sysdate)>=v_date)
				-->
				and cr.date_begin<=v_date
				and (cr.date_close is null or cr.date_close>v_date)
				and cr.kind_credit.name!='Технический овердрафт'
				and cr.com_status.name in ('Работает','Закрыт')
				and cr%id = part.[PRODUCT]
				and part%collection = zal.[PART_TO_LOAN]
				and zal.[VID_GUARANTEE] = guar%id
				
				and guar%id !=306643919 --.[NAME] != 'ТМЦ'
				--and cr.[client].name = 'ОсОО "К-Инвест Плюс" (K-Invest Plus)'
				
		group by
				  cr.[CLIENT].[VIDS_CL].[CATEGORY].[NAME]
				 ,zal.num_dog
			     ,zal.date_begin	
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].valuta.cur_short
				 ,guar.name	
		         ,nvl(zal.[ZALOG_BODY]->[ZALOG_BODY].[N_COUNT], 1)
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[SUMMA]      		
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[DISCONT]    	
		         ,::[DOCUMENT].[LIB_CUR].get_rate(zal.[ZALOG_BODY]->[ZALOG_BODY].[VALUTA], v_date /*zal.[ZALOG_BODY]->[ZALOG_BODY].[DOCUM_DATE]*/)
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[STORAGE]	
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[NAME]
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[ID_CODE]
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[USEFUL_AREA]
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[TOTAL_AREA]
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[LIVING_AREA]
		         ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[LAND_AREA]
 			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[ADDRESS].[IMP_STR]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[owner].name
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[MARKA]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[GOS_NUMB]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[YEARTS]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[COLOR]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[KUZ]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[NUMDV]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[VOL]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[TIPTS]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[PTSN]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[DATE_PTS]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_CARRIERS)[ORG_TEHPAS].[NAME]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(Z_DEPOSIT)[DOGOVOR].[NUM_DOG]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(Z_DEPOSIT)[DOGOVOR].[account].main_usv.num
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(Z_DEPOSIT)[DOGOVOR].[account].main_v_id
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]
			     ,zal.[ZALOG_BODY]->[ZALOG_BODY].[owner_right].name
			     ,zal%id				
				 ,zal.user_zalog
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[kind_zal_body].[CODE]
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[SAFING_COMP].[NAME]	
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY].[SAFING_COMP]
				 ,zal.[ZALOG_BODY]->[ZALOG_BODY]%id
				--,cr.KIND_CREDIT.name
				--,cr%id
		
		
	)loop
	
	debug_pipe('Залог cr.z_id := '||cr.z_id ||' INN :=  '||cr.n_id, 0);
	vCodeID := cr.n_id;
	inn_cnt := 0;
	--if cr.n_id!='NULL' then --#1
	if vTblZalBody.count>0 then  --#2
		for ii in vTblZalBody.first..vTblZalBody.last where vTblZalBody(ii).zal_num_id= cr.n_id
		loop
			inn_cnt := 1;
		end loop;
		if inn_cnt>0 then
			null;
		else
			if cr.n_id!='NULL' then
				CurShort	:= cr.fin;
				SumNat		:= 0;
				
				vTblZalBody(vIdx).zal_body_id := cr.zb_id;
				vTblZalBody(vIdx).zal_id := cr.z_id;
				vTblZalBody(vIdx).zal_num_id := cr.n_id;
				
				Summ := GetZalRealSum2(cr.n_id, CurShort, SumNat);
				excel.write(row, 26, Summ);
				excel.write(row, 27, SumNat);
				excel.write(row, 28, 'ИНН ЕСТЬ Ф');
				vIdx := vIdx+1;
			else
				v_couter := 0;
				/*for( select cc(
						cc.cost															: cot
					)IN	::[ZLK_M_CR_ZAL_LOG],(::[ZALOG_BODY] all :a2),(::[zalog_values] all :a4), (::[zalog] all :a3), (::[ZLK_M_CRD_ZALOG] all :crd_zal),
						(::[PR_CRED] all : pr_cr), ([PART_TO_LOAN] all :prt_loan) all
					where
						a3%ID =cr.z_id
						and a2%id=cr.zb_id
						and cc.va = a2%id
						and a3.zalog_body = a4%collection
						and a4.zalog_body = a2%id
						and crd_zal.[GUARANTEE]=cr.z_id
						and cc.[CRED_ZALOG]=crd_zal%id
						and prt_loan.[PRODUCT]=pr_cr%id
						and a3.[PART_TO_LOAN]=prt_loan%collection
						and pr_cr.date_begin<=v_date
						and (nvl(pr_cr.date_close, sysdate)>=v_date)
						and pr_cr.com_status.name in ('Работает','Закрыт')
						
						--and crd_zal.[CREDIT]=cr.ID_
				)loop
				--debug_pipe('cr.z_id := '||cr.z_id, 0);
					if cc.cot is not null and cc.cot >=0 then
					    excel.write(row, 26, cc.cot);
					    excel.write(row, 27, cc.cot *cr.kurs);
					    excel.write(row, 28, 'ИНН ПУСТО РС');
					    v_couter :=  v_couter +1;
					end if;
				end loop;*/
				for( select cr2(
						--cc.cost															: cot
						--zl_body.[SUMMA]														:zl_summ,
						(select zl_log(zl_log.[COST]:cost) in ::[ZLK_M_CR_ZAL_LOG], ([ZLK_M_CRD_ZALOG] all :cr_zl) all where
							zl_log.[CRED_ZALOG]=cr_zl%id and cr_zl.[GUARANTEE]=zalog%id and cr_zl.[CREDIT]=cr2%id and zl_log.[VA]=zl_body%id
							and nvl(zl_log.[UPDATED], sysdate)<=to_date('01/02/2020')
							and nvl(zl_log.[CREATED], sysdate)<=to_date('01/02/2020') and rownum=1)		:cot
					)IN	::[PR_CRED], (::[ZALOG_BODY] all :zl_body),(::[zalog_values] all :zal_val), (::[zalog] all :zalog),
						([PART_TO_LOAN] all :prt_loan) all
					where
						zalog%ID =cr.z_id
						and zl_body%id=cr.zb_id
						and zalog.zalog_body = zal_val%collection
						and zal_val.zalog_body = zl_body%id
						and prt_loan.[PRODUCT]=cr2%id
						and zalog.[PART_TO_LOAN]=prt_loan%collection
						and cr2.date_begin<=v_date
						and (nvl(cr2.date_close, sysdate)>=v_date)
						and cr2.com_status.name in ('Работает','Закрыт')
						order by zalog%ID --desc
				)loop
					if cr2.cot is not null and cr2.cot >=0 then
					    excel.write(row, 26, cr2.cot);
					    excel.write(row, 27, cr2.cot * cr.kurs);
					    excel.write(row, 28, 'ИНН ПУСТО НЕ 1 РС');
					    v_couter :=  v_couter +1;
					end if;
				end loop;
				test1 := nvl(cr.zb_sm, 0)* cr.coun;
					if v_couter = 0 and test1 >= 0 then
						excel.write(row, 26, test1);
						excel.write(row, 27, test1 * cr.kurs);
						excel.write(row, 28, 'ИНН ПУСТО НЕ 1 ОЦ');
					elsif v_couter = 0 and test1 < 0 then	
						excel.write(row, 26, nvl(cr.zb_sm, 0)* cr.coun * nvl(cr.ds ,0)/100);
						excel.write(row, 27, (nvl(cr.zb_sm, 0)* cr.coun * nvl(cr.ds ,0)/100)* cr.kurs);
						excel.write(row, 28, 'ИНН ПУСТО НЕ 1 3');
					end if;
			end if;
			
		end if;
		
	else  --#2
		if cr.n_id!='NULL' then
			CurShort	:= cr.fin;
			SumNat		:= 0;
			
			vTblZalBody(vIdx).zal_body_id := cr.zb_id;
			vTblZalBody(vIdx).zal_id := cr.z_id;
			vTblZalBody(vIdx).zal_num_id := cr.n_id;
			debug_pipe('GGHGH We are in here!!!', 0);
			Summ := GetZalRealSum2(cr.n_id, CurShort, SumNat);
			excel.write(row, 26, Summ);
			excel.write(row, 27, SumNat);
			excel.write(row, 28, 'первый');
			vIdx := vIdx+1;
		else
			v_couter := 0;
			/*
				for( select cc(
						cc.cost															: cot
					)IN	::[ZLK_M_CR_ZAL_LOG],(::[ZALOG_BODY] all :a2),(::[zalog_values] all :a4), (::[zalog] all :a3), (::[ZLK_M_CRD_ZALOG] all :crd_zal),
						(::[PR_CRED] all : pr_cr), ([PART_TO_LOAN] all :prt_loan) all
					where
						a3%ID =cr.z_id
						and a2%id=cr.zb_id
						and cc.va = a2%id
						and a3.zalog_body = a4%collection
						and a4.zalog_body = a2%id
						and crd_zal.[GUARANTEE]=cr.z_id
						and cc.[CRED_ZALOG]=crd_zal%id
						and prt_loan.[PRODUCT]=pr_cr%id
						and a3.[PART_TO_LOAN]=prt_loan%collection
						and pr_cr.date_begin<=v_date
						and (nvl(pr_cr.date_close, sysdate)>=v_date)
						and pr_cr.com_status.name in ('Работает','Закрыт')
						
						--and crd_zal.[CREDIT]=cr.ID_
				)loop
				--debug_pipe('cr.z_id := '||cr.z_id, 0);
					if cc.cot is not null and cc.cot >=0 then
					    excel.write(row, 26, cc.cot);
					    excel.write(row, 27, cc.cot *cr.kurs);
					    excel.write(row, 28, 'ИНН ПУСТО РС');
					    v_couter :=  v_couter +1;
					end if;
				end loop;*/
				for( select cr2(
						--cc.cost															: cot
						--zl_body.[SUMMA]														:zl_summ,
						(select zl_log(zl_log.[COST]:cost) in ::[ZLK_M_CR_ZAL_LOG], ([ZLK_M_CRD_ZALOG] all :cr_zl) all where
							zl_log.[CRED_ZALOG]=cr_zl%id and cr_zl.[GUARANTEE]=zalog%id and cr_zl.[CREDIT]=cr2%id and zl_log.[VA]=zl_body%id
							and nvl(zl_log.[UPDATED], sysdate)<=to_date('01/02/2020')
							and nvl(zl_log.[CREATED], sysdate)<=to_date('01/02/2020') and rownum=1)		:cot
					)IN	--::[ZLK_M_CR_ZAL_LOG],
						::[PR_CRED], (::[ZALOG_BODY] all :zl_body),(::[zalog_values] all :zal_val), (::[zalog] all :zalog),
						([PART_TO_LOAN] all :prt_loan) all
						--(::[ZALOG_BODY] all :a2),(::[zalog_values] all :a4), (::[zalog] all :a3),
						--(::[ZLK_M_CRD_ZALOG] all :crd_zal),
						--(::[PR_CRED] all : pr_cr), ([PART_TO_LOAN] all :prt_loan)
						--all
					where
						zalog%ID =cr.z_id
						and zl_body%id=cr.zb_id
						--and cc.va = a2%id
						and zalog.zalog_body = zal_val%collection
						and zal_val.zalog_body = zl_body%id
						--and crd_zal.[GUARANTEE]=a3%ID
						--and cc.[CRED_ZALOG]=crd_zal%id
						--and crd_zal.[CREDIT]=cr.ID_
						and prt_loan.[PRODUCT]=cr2%id
						and zalog.[PART_TO_LOAN]=prt_loan%collection
						and cr2.date_begin<=v_date
						and (nvl(cr2.date_close, sysdate)>=v_date)
						and cr2.com_status.name in ('Работает','Закрыт')
						order by zalog%ID --desc
				)loop
				--debug_pipe('cr.z_id := '||cr.z_id, 0);
					if cr2.cot is not null and cr2.cot >=0 then
					    excel.write(row, 26, cr2.cot);
					    excel.write(row, 27, cr2.cot *cr.kurs);
					    excel.write(row, 28, 'ИНН ПУСТО РС');
					    v_couter :=  v_couter +1;
					end if;
				end loop;
				
				test1 := nvl(cr.zb_sm, 0)* cr.coun;
					if v_couter = 0 and test1 >= 0 then
						excel.write(row, 26, test1);
						excel.write(row, 27, test1 * cr.kurs);
						excel.write(row, 28, 'ИНН ПУСТОЙ 2');
					elsif v_couter = 0 and test1 < 0 then	
						excel.write(row, 26, nvl(cr.zb_sm, 0)* cr.coun * nvl(cr.ds ,0)/100);
						excel.write(row, 27, (nvl(cr.zb_sm, 0)* cr.coun * nvl(cr.ds ,0)/100)* cr.kurs);
						excel.write(row, 28, 'ИНН ПУСТОЙ 3');
					end if;
		end if;
		
	end if;
	
	/*else  --#1
				v_couter := 0;
				for( select cc(
						cc.cost															: cot
					)IN	::[ZLK_M_CR_ZAL_LOG],(::[ZALOG_BODY] all :a2),(::[zalog_values] all :a4), (::[zalog] all :a3) all
					where
						a3%ID =cr.z_id
						and a2%id=cr.zb_id
						and cc.va = a2%id
						and a3.zalog_body = a4%collection
						and a4.zalog_body = a2%id
				)loop
					if cc.cot is not null and cc.cot >=0 then
					    excel.write(row, 26, cc.cot);
					    excel.write(row, 27, cc.cot *cr.kurs);
					    excel.write(row, 28, 'ИНН ПУСТО РС');
					    v_couter :=  v_couter +1;
					end if;
				end loop;
				test1 := nvl(cr.zb_sm, 0)* cr.coun;
					if v_couter = 0 and test1 >= 0 then
						excel.write(row, 26, test1);
						excel.write(row, 27, test1 * cr.kurs);
						excel.write(row, 28, 'ИНН ПУСТОЙ 2');
					elsif v_couter = 0 and test1 < 0 then	
						excel.write(row, 26, nvl(cr.zb_sm, 0)* cr.coun * nvl(cr.ds ,0)/100);
						excel.write(row, 27, (nvl(cr.zb_sm, 0)* cr.coun * nvl(cr.ds ,0)/100)* cr.kurs);
						excel.write(row, 28, 'ИНН ПУСТОЙ 3');
					end if;
	end if;	*/
			excel.write(row, 29, cr.zb_sm);
			excel.write(row, 30, cr.coun);
			excel.write(row, 31, cr.ds);

	id_zal := cr.z_id;
	 begin
	     select p( cr.[HIGH_LEVEL_CR]
	     )in ::[PART_TO_LOAN],(::[PR_CRED] all : cr) all
	     where p%collection = id_zal.[PART_TO_LOAN]
	     and   cr%id = p.[PRODUCT]
	     and rownum<2
	     into credit;
	 exception
	 	when no_data_found then
	 	credit:= null;
	 end;
	
	
	
	
	
	 if credit is not null then
	 		for (select prop_in(prop_in%id						: id_,
	 							prop_in.[DEPART].[NAME]			: fil,	
	 							prop_in.[NUM_DOG] 				: num,
	 							prop_in.[CLIENT].[NAME]			: cl
								)in ::[PR_CRED] all	
			 					where prop_in.[PROPERTIES].[GROUP_PROP] =  -- проверка по группе
			 											(select prop(prop.[properties].[GROUP_PROP] : g_id
														 )in ::[PR_CRED] all	
														 where prop%id = credit
														 and  prop.[PROPERTIES].[GROUP_PROP].[CODE] = 'CRED_TRANCH')
								 and  prop_in.[PROPERTIES].[GROUP_PROP].[code] = 'CRED_TRANCH'
								 and  prop_in.[CLIENT] = --проверка по клиенту
								 						 (select prop(prop.[CLIENT]  			    : prop_cl
														  )in ::[PR_CRED] all	
														   where prop%id = credit
														   and  prop.[PROPERTIES].[GROUP_PROP].[CODE] = 'CRED_TRANCH')
								 and prop_in.[PROPERTIES].[STR] in   --перекрестная проверка по номеру договору			
								                                (select prop(prop.[NUM_DOG]			:prop_nd
														  		)in ::[PR_CRED] all	
														   		where prop%id = credit
														   		and  prop.[PROPERTIES].[GROUP_PROP].[CODE] = 'CRED_TRANCH'
														   		and	 prop.[COM_STATUS].[CODE] = 'WORK'
														   		union all
														   		select prop(prop.[PROPERTIES].[STR]			:prop_str
														  		)in ::[PR_CRED] all	
														   		where prop%id = credit
														   		and  prop.[PROPERTIES].[GROUP_PROP].[CODE] = 'CRED_TRANCH'
														   		and	 prop.[COM_STATUS].[CODE] = 'WORK'
														   		)
								 and	 prop_in.[COM_STATUS].[CODE] = 'WORK'		
								 and rownum<2				   		
			 )loop
			  excel.write(row,  1, prop_in.id_);
			  excel.write(row,  2, prop_in.fil);
			 -- excel.write(row,  3, prop_in.num);
			  excel.write(row,  3, prop_in.cl);			
			 end loop;
			 begin
			     select p( cr%id
			     )in ::[PART_TO_LOAN],(::[PR_CRED] all : cr) all
			     where p%collection = id_zal.[PART_TO_LOAN]
			     and   cr%id = p.[PRODUCT]
			     and rownum<2
			     into credit;
			 exception
			 	when no_data_found then
			 		null;
		     end;
		     excel.write(row,  1, credit%id);
		     excel.write(row,  2, credit.[DEPART].[NAME]);
		     --excel.write(row,  3, credit.[NUM_DOG]);
		     excel.write(row,  3, credit.[CLIENT].[NAME]);
			
	 else
     		begin
			     select p( cr%id
			     )in ::[PART_TO_LOAN],(::[PR_CRED] all : cr) all
			     where p%collection = id_zal.[PART_TO_LOAN]
			     and   cr%id = p.[PRODUCT]
			     and rownum<2
			     into credit;
			 exception
			 	when no_data_found then
			 		null;
		     end;
		     excel.write(row,  1, credit%id);
		     excel.write(row,  2, credit.[DEPART].[NAME]);
		     --excel.write(row,  3, credit.[NUM_DOG]);
		     excel.write(row,  3, credit.[CLIENT].[NAME]);
		
	 end if;
	
	
	 excel.write(row,  4, cr.cotegory);
	 excel.write(row,  6, cr.nd);
	 excel.write(row,  7, to_char(cr.dt,'dd.mm.yyyy'));
	 excel.write(row, 8, cr.fin);
     excel.write(row, 9, cr.nm);
	 excel.write(row, 10, cr.zb_sm);
	 excel.write(row, 11, cr.coun);
	 excel.write(row, 12, cr.ds);
	 excel.write(row, 13, cr.zb_sm*cr.ds/100*cr.coun);
	 excel.write(row, 14, cr.zb_sm*cr.ds/100*cr.kurs*cr.coun);
	 excel.write(row, 15, cr.adr);
	 excel.write(row, 16, cr.op);
	 excel.write(row, 20, '   '); --номер полюса (пока пустое)
	 excel.write(row, 21, cr.komp);  -- страхование , компание
	 --excel.write(row, 25, cr.k_cred);  --вид кредита

	 for( select s( s.[DATE_BEG]	: db,
	 				s.[DATE_END]	: de
	
	 ) in ::[ZLK_INSURANCE],(::[ZALOG] all :zal) all --ERBOL
	   where s.[ZALOG_BODY_ARR] =  zal.[ZALOG_BODY]->[ZALOG_BODY]%id
	   and   zal%id = id_zal
	 )loop
	  excel.write(row, 22, to_char(s.db,'dd.mm.yyyy'));
	  excel.write(row, 23, to_char(s.de,'dd.mm.yyyy'));
	 end loop;
	
	
	 if cr.n_us is not null then str3:=cr.n_id; end if;
	 If cr.n_tot is not null then str:=str||' Общ.пл.:'||cr.n_tot; end if;
	 if cr.n_us is not null then 	str:=str||' Пол.пл.:'||cr.n_us; end if;
	 if cr.n_liv is not null then str:=str||' Жил.пл.:'||cr.n_liv; end if;
	 if cr.n_ar is not null then str:=str||' Зем.уч.:'||cr.n_ar; end if;
	-----
	 if cr.a_mark is not null then str:=str||' Марка:'||cr.a_mark; end if;
	 if cr.a_gos is not null then str:=str||' Гос.ном:'||cr.a_gos;end if;
	 if cr.a_year is not null then str:=str||' Год:'||cr.a_year;end if;
	 if cr.a_col is not null then str:=str||' Цвет:'||cr.a_col;end if;
	 if cr.a_kuz is not null then str:=str||' Кузов:'||cr.a_kuz;end if;
	 if cr.a_dv is not null then str:=str||' Двиг.:'||cr.a_dv;end if;
	 if cr.a_vol is not null then str:=str||' Объем:'||cr.a_vol;end if;
	 if cr.a_tip is not null then str:=str||' Тип.:'||cr.a_tip;end if;
	 if cr.a_pt is not null then 	str:=str||' Тех.пт:'||cr.a_pt;end if;
	 if cr.a_dt is not null then 	str:=str||' Дата.тех:'||cr.a_dt;end if;
	 if cr.a_tech is not null then str:=str||' Орг.выдан:'||cr.a_tech;end if;
	 if cr.depn is not null then str:=str||' Депозит:'||cr.depn;end if;
	 excel.write(row, 17, str3);
	 excel.write(row, 18, str);
	 str2:=null; tmp:=null;
	 if cr.nm like 'Недвиж%' then null;
		elsif cr.nm like 'Авто%' then tmp:=cr.a_cli;
		elsif cr.nm like 'Поруч%' then  tmp:=cr.op;
		elsif cr.nm not like 'Недвиж%' and cr.nm not like 'Авто%' and cr.nm not like 'Поруч%' then
			for (
			 		select z1 (
			 		        z1.[users_zalog].[DEBTOR].[name]:cli
						   )
			 		   in ::[zalog]
			 		
					where
					z1%id=cr.z_id and z1.[users_zalog].role.code='SOPLEDGER'
																		
				) loop
				If z1.cli is not null then
				tmp:=tmp||' '||z1.cli;
				end if;
				end loop;
		end if;
		if tmp is null then tmp:=cr.zd.name; end if;
			-----
			for (
		 		select z1 (
		 		        z1.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[DOC_DESCRIPTIONS].[NUM_DOG]:numb
					   ,z1.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[DOC_DESCRIPTIONS].[DOC_DESCRIPTION]:name
					   ,z1.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[DOC_DESCRIPTIONS].[DATE_GIV]:dateg	
					   ,z1.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]->(ZBADD_HOUSE)[DOC_DESCRIPTIONS].[client].name:cli
					    )
		 		   in ::[zalog]
		 		
				where
				z1%id=cr.z_id
				and z1.[ZALOG_BODY]->[ZALOG_BODY].[ADDS]=cr.zl
									
			) loop
			If z1.numb is not null then
			str2:=str2||' '||z1.name||' № '||z1.numb||' выдан от '||to_char(z1.dateg,'dd.mm.yyyy')||';';
			tmp:=z1.cli;
			end if;
			end loop;
			excel.write(row, 5, tmp);
			excel.write(row, 19, str2);
			
			if cr.kind_body_code='REAL_UNI' then
				excel.write(row, 24, 'Да');
			end if;
	 str=str3=str2:=null;
	 row:=row+1;
	 end loop;

	
end;
