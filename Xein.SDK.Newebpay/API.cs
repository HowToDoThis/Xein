using System.Collections.Specialized;
using System.Text;

namespace Xein.SDK.Newebpay;

public static class API
{
    static MerchantConfig _config;
    
    public static void SetConfig(MerchantConfig config)
    {
        _config = config;
    }

    private static NameValueCollection BuildTradeInfo(string orderNo, string amount, string itemDesc)
    {
        var queries = System.Web.HttpUtility.ParseQueryString(string.Empty);
        
        // Head
        queries.Add("MerchantID",  _config.MerchantID);
        queries.Add("RespondType", "JSON");
        queries.Add("TimeStamp",   (DateTime.Now - DateTime.UnixEpoch).Ticks.ToString());
        queries.Add("Version",     "2.0");
        // URLs
        queries.Add("NotifyURL",   "http://xein.tplinkdns.com/newebpay/notify");
        queries.Add("ReturnURL",   "http://xein.tplinkdns.com/newebpay/return");
        // Item Info
        queries.Add("MerchantOrderNo", string.IsNullOrEmpty(orderNo)  ? MerchantConfig.TestMerchantOrder : orderNo);
        queries.Add("Amt",             string.IsNullOrEmpty(amount)   ? MerchantConfig.TestAmount : amount);
        queries.Add("ItemDesc",        string.IsNullOrEmpty(itemDesc) ? MerchantConfig.TestItemDesc : itemDesc);
        // PaymentType
        queries.Add("WEBATM",  _config.WebATM ? "1" : "0");
        queries.Add("VACC",    _config.VACC ? "1" : "0");
        queries.Add("CVS",     _config.CVS ? "1" : "0");
        queries.Add("BARCODE", _config.BARCODE ? "1" : "0");

        return queries;
    }
    
    public static string MPG(string orderNo, string amount, string itemDesc)
    {
        var rawTradeInfo = BuildTradeInfo(orderNo, amount, itemDesc).ToString();
        var tradeInfo    = Encoding.UTF8.GetString(Crypto.EncryptAES(Encoding.UTF8.GetBytes(rawTradeInfo), Encoding.UTF8.GetBytes(_config.Key), Encoding.UTF8.GetBytes(_config.IV)));

        var rawSha   = $"HashKey={_config.Key}&{tradeInfo}&HashIV={_config.IV}";
        var tradeSha = Encoding.UTF8.GetString(Crypto.EncryptSHA(Encoding.UTF8.GetBytes(rawSha))).ToUpper();

        List<KeyValuePair<string, string>> postData = [
            new("MerchantID", _config.MerchantID),
            new("TradeInfo", tradeInfo),
            new("TradeSha", tradeSha),
            new("Version", "2.0"),
        ];

        var response = new HttpClient()
                       .PostAsync($"{_config.GetAPI}/MPG/mpg_gateway"
                                  , new FormUrlEncodedContent(postData))
                       .GetAwaiter()
                       .GetResult();

        return response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
    }
}
