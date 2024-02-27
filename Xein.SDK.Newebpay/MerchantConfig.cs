namespace Xein.SDK.Newebpay;

public class MerchantConfig
{
    public const string TestGatewayURL = "https://ccore.newebpay.com/";
    public const string MainGatewayURL = "https://core.newebpay.com/";

    /*
     * 測試串接網址：https://ccore.newebpay.com/MPG/mpg_gateway
     * 正式串接網址：https://core.newebpay.com/MPG/mpg_gateway
     */
    public bool IsTesting { get; set; }
    public string GetAPI => IsTesting ? TestGatewayURL : MainGatewayURL;
    
    public string MerchantID { get; set; }
    public string Key        { get; set; }
    public string IV         { get; set; }
    
    public string NotifyURL { get; set; }
    public string ReturnURL { get; set; }
    
    public bool WebATM  { get; set; }
    public bool VACC    { get; set; }
    public bool CVS     { get; set; }
    public bool BARCODE { get; set; }

    public static string TestMerchantOrder => $"test_{DateTime.Now.Year}_{DateTime.Now.Month}_{DateTime.Now.Day}_{DateTime.Now.TimeOfDay.Ticks}";
    public static string TestAmount        => "1";
    public static string TestItemDesc      => "testItem";
    
}