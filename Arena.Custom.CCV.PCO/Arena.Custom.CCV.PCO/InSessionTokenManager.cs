using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Web;
using DotNetOpenAuth.OAuth.ChannelElements;
using DotNetOpenAuth.OAuth.Messages;

namespace Arena.Custom.CCV.PCO
{
    public class InSessionTokenManager : IConsumerTokenManager
    {
        #region IConsumerTokenManager Members

        public string ConsumerKey { get; set; }
        public string ConsumerSecret { get; set; }

        #endregion

        #region ITokenManager Members

        public void ExpireRequestTokenAndStoreNewAccessToken(string consumerKey, string requestToken, string accessToken, string accessTokenSecret)
        {
            HttpContext.Current.Session.Remove(requestToken);
            HttpContext.Current.Session[accessToken] = accessTokenSecret;
        }

        public string GetTokenSecret(string token)
        {
            return (string)HttpContext.Current.Session[token];
        }

        public TokenType GetTokenType(string token)
        {
            throw new NotImplementedException();
        }

        public bool IsRequestTokenAuthorized(string requestToken)
        {
            throw new NotImplementedException();
        }

        public void StoreNewRequestToken(UnauthorizedTokenRequest request, ITokenSecretContainingMessage response)
        {
            HttpContext.Current.Session[response.Token] = response.TokenSecret;
        }

        #endregion
    }
}
