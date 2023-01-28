from enum import Enum
from typing import Union, List

"""
对F1的相关依赖,方便后续升级,直接替换即可
"""
cdn_num_ls = [1] * 4


class RankAscending(Enum):
    TRUE = True
    FALSE = False


class FilterAfter(Enum):
    TRUE = True
    FALSE = False


def parallel_filter_handle(filter_before_exec):
    '''
    将默认的串联过滤转化为并联过滤,只使用于filter_generate生成的过滤逻辑,后置过滤不适用,默认并联
    '''
    series_filter_list = []
    for content in filter_before_exec:
        series_filter_list += content.split('\n')
    define_strs_list = [x for x in series_filter_list if 'loc' not in x]
    filter_strs_list = [x for x in series_filter_list if 'loc' in x]
    parallel_filter_list = define_strs_list + filter_strs_list
    return parallel_filter_list, 'between'


def filter_generate(direction: str = 'long', filter_factor: str = '涨跌幅max_fl_24', filter_type: str = 'value',
                    compare_operator: str = 'lt', filter_value: Union[int, float, List[Union[int, float]]] = 0.2,
                    rank_ascending: bool = False, filter_after: bool = False, weight_ratio: float = 0,
                    param: list = None) -> str:
    """
    : param direction: 过滤的方向  '多'/'long'/'df1'或 '空'/'short'/'df2'
    : param filter_factor: 过滤因子名 如 '涨跌幅max_fl_24'
    : param filter_type: 过滤方式 value/rank/pct  原始数值/排名(默认从大到小)/百分位(从小到大)
    : param filter_value: 过滤阈值 支持 int float list
    : param compare_operator: 和数值的比较关系 lt gt bt nbt lte gte bte nbte eq ne
    : param rank_ascending: True/False 控制 rank模式下的排名方向,对pct无效
    : param filter_after: False/True 是否为后置过滤
    : param weight_ratio: 被后置的币设定资金系数 0 即是清仓
    : param inclusive:  True 闭区间 ； Flase 开区间
    : param param: [direction,filter_factor,filter_type,filter_value,compare_operator,rank_ascending,filter_after,weight_ratio] 的前5到8个元素 便于链式过滤传参
    : param compare_operator 详解:
        lt, gt, lte, gte, bt, bte, nbt, nbte 是一些缩写,它们在数学和计算机科学中有特定的含义。
        lt 是 less than 的缩写,表示“小于”。
        gt 是 greater than 的缩写,表示“大于”。
        lte 是 less than or equal to 的缩写,表示“小于等于”。
        gte 是 greater than or equal to 的缩写,表示“大于等于”。
        bt 是 between 的缩写,表示“介于两者之间”。
        bte 是 between, inclusive 的缩写,表示“介于两者之间,包括两者”。
        nbt 是 not between 的缩写,表示“不介于两者之间”。
        nbte 是 not between, inclusive 的缩写,表示“不介于两者之间,但包括两者”。
        eq 是 equal 的缩写,表示“等于”
        ne 是 not equal 的缩写,表示“不等于”
    """

    # 生成过滤行为组件
    def _str_generate(param: list = None) -> tuple:
        if len(param) < 5:
            raise ValueError('Wrong param length!')
        else:
            direction, filter_factor, filter_type, compare_operator, filter_value, rank_ascending, filter_after, \
                weight_ratio = param + [False, False, 0][len(param) - 5:3]
        cdn_map = {'df1': 0, 'df2': 1}
        direction_map = {'多': 'df1', 'long': 'df1', 'df1': 'df1',
                         '空': 'df2', 'short': 'df2', 'df2': 'df2'}
        dfx = direction_map.get(direction)

        if dfx is None:
            raise ValueError('Wrong direction!')

        assert filter_type in ['value', 'rank', 'pct']
        assert compare_operator in ['lt', 'lte', 'gt', 'gte', 'bt', 'bte', 'nbt', 'nbte', 'eq', 'ne']
        assert type(filter_factor) == str
        if rank_ascending in RankAscending.__members__.values():
            rank_ascending = rank_ascending.value
        if filter_after in FilterAfter.__members__.values():
            filter_after = filter_after.value
        assert rank_ascending in [True, False]
        assert filter_after in [True, False]
        if compare_operator == 'eq':
            assert type(filter_value) in [float, int]
            compare_operator = 'bte'
            filter_value = [filter_value, filter_value]
        elif compare_operator == 'ne':
            assert type(filter_value) in [float, int]
            compare_operator = 'nbt'
            filter_value = [filter_value, filter_value]
        if compare_operator.endswith('e'):
            inclusive = True
        else:
            inclusive = False
        if filter_type == 'pct': rank_ascending = True
        use_pct = None
        if compare_operator in ['lt', 'lte', 'gt', 'gte']:
            if filter_type == 'rank':
                assert type(filter_value) == int
                use_pct = False
            elif filter_type == 'pct':
                assert 0 <= filter_value <= 1
                use_pct = True
            else:
                assert type(filter_value) in [float, int]
        else:
            assert type(filter_value) == list
            assert filter_value[0] <= filter_value[1]
            if filter_type == 'rank':
                assert type(filter_value[0]) == int
                assert type(filter_value[1]) == int
                use_pct = False
            elif filter_type == 'pct':
                assert 0 <= filter_value[0] <= 1
                assert 0 <= filter_value[1] <= 1
                use_pct = True
        if type(filter_value) != list:
            if compare_operator in ['lt', 'lte']:
                filter_value = [-1e100, filter_value]
            elif compare_operator in ['gt', 'gte']:
                filter_value = [filter_value, 1e100]
        if compare_operator[:2] == 'nb':
            inclusive = not inclusive
            reverse = '~'
        else:
            reverse = ''
        if filter_type == 'value':
            rank_str = f"filter_factor = ['{filter_factor}'][0]"
        else:
            rank_str = f"{dfx}[f'{filter_factor}_rank'] = {dfx}.groupby('candle_begin_time')['{filter_factor}'].rank(method='first', pct={use_pct}, ascending={rank_ascending})"
            filter_factor = f'{filter_factor}_rank'
        left, right = filter_value
        pre_fix = 'long_' if dfx == 'df1' else 'short_'
        map_ad = 0 if not filter_after else 2
        num = cdn_num_ls[cdn_map[dfx] + map_ad]
        condition_str = f"{pre_fix}condition{num} = {reverse}{dfx}[f'{filter_factor}'].between({left},{right},inclusive={inclusive})"
        cdn_num_ls[cdn_map[dfx] + map_ad] += 1

        return rank_str, condition_str, dfx, num, weight_ratio

    # 数字映射，解决计数替换重码
    chinese_digit = '零 一二三四五六七八九'
    digit_map = {str(i): v for i, v in enumerate(chinese_digit)}
    # 组件构装
    if param is None:
        param = direction, filter_factor, filter_type, compare_operator, filter_value, rank_ascending, filter_after, weight_ratio
    if type(param) == list:
        filter_after = False if len(param) < 7 else param[6]
        if rank_ascending in RankAscending.__members__.values():
            rank_ascending = rank_ascending.value
        if filter_after in FilterAfter.__members__.values():
            filter_after = filter_after.value
        try:
            rank_str, condition_str, dfx, num, weight_ratio = _str_generate(param)
        except Exception as e:
            print('出错参数：', param)
            raise e
        pre_fix = 'long_' if dfx == 'df1' else 'short_'
        if not filter_after:
            filter_str = f"{dfx} = {dfx}.loc[{pre_fix}condition{num}]"
        else:
            filter_str = f"{dfx}.loc[{pre_fix}condition{num},'weight_ratio'] = {weight_ratio}"
        filter_str = f"""{rank_str}\n{condition_str}\n{filter_str}"""
        return filter_str
    elif type(param) == tuple:
        *params_list, logical_operators = param
        param = params_list[0]
        filter_after = False if len(param) < 7 else param[6]
        if filter_after in FilterAfter.__members__.values():
            filter_after = filter_after.value
        assert type(logical_operators) == str
        filter_res_list = []
        for x in params_list:
            try:
                filter_res_list.append(_str_generate(x))
            except Exception as e:
                print('出错参数：', x)
                raise e
        if len(set([x[2] for x in filter_res_list])) != 1: raise ValueError('df1 与 df2 不能进行逻辑运算')
        ref = filter_res_list[0][3] - 1
        for i in range(10):
            logical_operators = logical_operators.replace(str(i), digit_map[str(i)])
        for i, filter_res in enumerate(filter_res_list[::-1]):
            i = len(filter_res_list) - i - 1
            dfx, num, weight_ratio = filter_res[2:]
            pre_fix = 'long_' if dfx == 'df1' else 'short_'
            raw_digit = digit_map[str(i + 1)]
            target_digit = str(i + 1 + ref)
            logical_operators = logical_operators.replace(raw_digit, f'{pre_fix}condition{target_digit}')
        if not filter_after:
            filter_str = f"{dfx} = {dfx}.loc[{logical_operators}]"
        else:
            if len(set([x[4] for x in filter_res_list])) != 1: raise ValueError(
                '后置过滤与或并运算，要求weight_ratio一致')
            filter_str = f"{dfx}.loc[{logical_operators},'weight_ratio'] = {weight_ratio}"
        filter_strs = []
        [filter_strs.extend(x[:2]) for x in filter_res_list]
        filter_strs += [filter_str]
        filter_str = '\n'.join(filter_strs)
        return f"""{filter_str}"""


def do_filter(df1, df2, exec_list):
    d = {'df1': df1, 'df2': df2}
    for content in exec_list:
        try:
            exec(content, globals(), d)
            df1 = d['df1']
            df2 = d['df2']
        except IndentationError as e:
            raise ValueError(f'{e}:', '请删掉过滤条件每行开头的缩进!')
    return df1, df2
