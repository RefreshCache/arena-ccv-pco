using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

using DotNetOpenAuth.Messaging;
using DotNetOpenAuth.OAuth;
using DotNetOpenAuth.OAuth.ChannelElements;

namespace Arena.Custom.CCV.PCO
{
    public class Consumer
    {
        public static readonly ServiceProviderDescription ServiceDescription = new ServiceProviderDescription
        {
            RequestTokenEndpoint = new MessageReceivingEndpoint("http://www.planningcenteronline.com/oauth/request_token", HttpDeliveryMethods.GetRequest | HttpDeliveryMethods.AuthorizationHeaderRequest),
            UserAuthorizationEndpoint = new MessageReceivingEndpoint("http://www.planningcenteronline.com/oauth/authorize", HttpDeliveryMethods.GetRequest | HttpDeliveryMethods.AuthorizationHeaderRequest),
            AccessTokenEndpoint = new MessageReceivingEndpoint("http://www.planningcenteronline.com/oauth/access_token", HttpDeliveryMethods.GetRequest | HttpDeliveryMethods.AuthorizationHeaderRequest),
            TamperProtectionElements = new ITamperProtectionChannelBindingElement[] { new HmacSha1SigningBindingElement() },
        };

        public static Uri GetRequestURL(IConsumerTokenManager tokenManager, string account)
        {
            DesktopConsumer desktopConsumer = new DesktopConsumer(ServiceDescription, tokenManager);

            Dictionary<string, string> requestParams = new Dictionary<string, string>();
            requestParams.Add("oauth_callback", string.Format("{0}/PCOResult.aspx?account={1}",
                Arena.Utility.ArenaUrl.FullApplicationRoot(), account));

            string requestToken = string.Empty;
            return desktopConsumer.RequestUserAuthorization(requestParams, null, out requestToken);
        }

        public static string GetAccessToken(
            IConsumerTokenManager tokenManager, string requestToken, string verifier)
        {
            DesktopConsumer desktopConsumer = new DesktopConsumer(ServiceDescription, tokenManager);

            var accessToken = desktopConsumer.ProcessUserAuthorization(requestToken, verifier);

            return string.Format("{0}={1}",
                accessToken.AccessToken,
                tokenManager.GetTokenSecret(accessToken.AccessToken));
        }
    }
}
