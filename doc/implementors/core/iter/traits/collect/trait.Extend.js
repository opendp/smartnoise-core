(function() {var implementors = {};
implementors["bytes"] = [{"text":"impl Extend&lt;u8&gt; for BytesMut","synthetic":false,"types":[]},{"text":"impl&lt;'a&gt; Extend&lt;&amp;'a u8&gt; for BytesMut","synthetic":false,"types":[]}];
implementors["csv"] = [{"text":"impl&lt;T:&nbsp;AsRef&lt;[u8]&gt;&gt; Extend&lt;T&gt; for ByteRecord","synthetic":false,"types":[]},{"text":"impl&lt;T:&nbsp;AsRef&lt;str&gt;&gt; Extend&lt;T&gt; for StringRecord","synthetic":false,"types":[]}];
implementors["either"] = [{"text":"impl&lt;L, R, A&gt; Extend&lt;A&gt; for Either&lt;L, R&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;L: Extend&lt;A&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;R: Extend&lt;A&gt;,&nbsp;</span>","synthetic":false,"types":[]}];
implementors["hashbrown"] = [{"text":"impl&lt;K, V, S&gt; Extend&lt;(K, V)&gt; for HashMap&lt;K, V, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;K: Eq + Hash,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;'a, K, V, S&gt; Extend&lt;(&amp;'a K, &amp;'a V)&gt; for HashMap&lt;K, V, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;K: Eq + Hash + Copy,<br>&nbsp;&nbsp;&nbsp;&nbsp;V: Copy,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;T, S&gt; Extend&lt;T&gt; for HashSet&lt;T, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Eq + Hash,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;'a, T, S&gt; Extend&lt;&amp;'a T&gt; for HashSet&lt;T, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: 'a + Eq + Hash + Copy,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]}];
implementors["indexmap"] = [{"text":"impl&lt;K, V, S&gt; Extend&lt;(K, V)&gt; for IndexMap&lt;K, V, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;K: Hash + Eq,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;'a, K, V, S&gt; Extend&lt;(&amp;'a K, &amp;'a V)&gt; for IndexMap&lt;K, V, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;K: Hash + Eq + Copy,<br>&nbsp;&nbsp;&nbsp;&nbsp;V: Copy,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;T, S&gt; Extend&lt;T&gt; for IndexSet&lt;T, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Hash + Eq,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;'a, T, S&gt; Extend&lt;&amp;'a T&gt; for IndexSet&lt;T, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Hash + Eq + Copy + 'a,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]}];
implementors["openssl"] = [{"text":"impl Extend&lt;CMSOptions&gt; for CMSOptions","synthetic":false,"types":[]},{"text":"impl Extend&lt;OcspFlag&gt; for OcspFlag","synthetic":false,"types":[]},{"text":"impl Extend&lt;Pkcs7Flags&gt; for Pkcs7Flags","synthetic":false,"types":[]},{"text":"impl Extend&lt;SslOptions&gt; for SslOptions","synthetic":false,"types":[]},{"text":"impl Extend&lt;SslMode&gt; for SslMode","synthetic":false,"types":[]},{"text":"impl Extend&lt;SslVerifyMode&gt; for SslVerifyMode","synthetic":false,"types":[]},{"text":"impl Extend&lt;SslSessionCacheMode&gt; for SslSessionCacheMode","synthetic":false,"types":[]},{"text":"impl Extend&lt;ExtensionContext&gt; for ExtensionContext","synthetic":false,"types":[]},{"text":"impl Extend&lt;ShutdownState&gt; for ShutdownState","synthetic":false,"types":[]},{"text":"impl Extend&lt;X509CheckFlags&gt; for X509CheckFlags","synthetic":false,"types":[]}];
implementors["proc_macro2"] = [{"text":"impl Extend&lt;TokenTree&gt; for TokenStream","synthetic":false,"types":[]},{"text":"impl Extend&lt;TokenStream&gt; for TokenStream","synthetic":false,"types":[]}];
implementors["serde_json"] = [{"text":"impl Extend&lt;(String, Value)&gt; for Map&lt;String, Value&gt;","synthetic":false,"types":[]}];
implementors["syn"] = [{"text":"impl&lt;T, P&gt; Extend&lt;T&gt; for Punctuated&lt;T, P&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;P: Default,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;T, P&gt; Extend&lt;Pair&lt;T, P&gt;&gt; for Punctuated&lt;T, P&gt;","synthetic":false,"types":[]},{"text":"impl Extend&lt;Error&gt; for Error","synthetic":false,"types":[]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()