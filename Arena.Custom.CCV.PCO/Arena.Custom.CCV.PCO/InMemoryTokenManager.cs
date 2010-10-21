using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using DotNetOpenAuth.OAuth.ChannelElements;
using DotNetOpenAuth.OAuth.Messages;

namespace Arena.Custom.CCV.PCO
{
    public class InMemoryTokenManager : IConsumerTokenManager
    {
        const string DEFAULT_CONSUMER_TOKEN = "7jVAULjCdp2FzSbmezO7";
        const string DEFAULT_CONSUMER_SECRET = "v7oLc9IzsCDABmWtPrWvwYzEg135Fyd06A7vPBRI";

        private Dictionary<string, string> TokenSecrets = new Dictionary<string, string>();

        public InMemoryTokenManager(int organizationId, Arena.Core.Lookup pcoAccount)
        {
            Arena.Organization.Organization organization = new Arena.Organization.Organization(organizationId);

            ConsumerKey = organization.Settings["PCO_Consumer_Token"] ??  DEFAULT_CONSUMER_TOKEN;
            ConsumerSecret = organization.Settings["PCO_Consumer_Secret"] ?? DEFAULT_CONSUMER_SECRET;

            if (pcoAccount != null)
            {
                string accessKey = pcoAccount.Qualifier;
                string accessSecret = pcoAccount.Qualifier8;

                if (accessKey != string.Empty && accessSecret != string.Empty)
                    TokenSecrets.Add(accessKey, accessSecret);
            }
        }

        #region IConsumerTokenManager Members

        public string ConsumerKey { get; set; }
        public string ConsumerSecret { get; set; }
        
        #endregion

        #region ITokenManager Members

        public void ExpireRequestTokenAndStoreNewAccessToken(string consumerKey, string requestToken, string accessToken, string accessTokenSecret)
        {
            TokenSecrets.Remove(requestToken);
            TokenSecrets[accessToken] = accessTokenSecret;
        }

        public string GetTokenSecret(string token)
        {
            return TokenSecrets[token];
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
            TokenSecrets[response.Token] = response.TokenSecret;
        }

        #endregion
    }
}
