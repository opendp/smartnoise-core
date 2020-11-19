(function() {var implementors = {};
implementors["itertools"] = [{"text":"impl&lt;'a, I&gt; Octal for Format&lt;'a, I&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;I: Iterator,<br>&nbsp;&nbsp;&nbsp;&nbsp;I::Item: Octal,&nbsp;</span>","synthetic":false,"types":[]}];
implementors["num_bigint"] = [{"text":"impl Octal for BigInt","synthetic":false,"types":[]},{"text":"impl Octal for BigUint","synthetic":false,"types":[]}];
implementors["num_complex"] = [{"text":"impl&lt;T&gt; Octal for Complex&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Octal + Num + PartialOrd + Clone,&nbsp;</span>","synthetic":false,"types":[]}];
implementors["num_rational"] = [{"text":"impl&lt;T:&nbsp;Octal + Clone + Integer&gt; Octal for Ratio&lt;T&gt;","synthetic":false,"types":[]}];
implementors["openssl"] = [{"text":"impl Octal for CMSOptions","synthetic":false,"types":[]},{"text":"impl Octal for OcspFlag","synthetic":false,"types":[]},{"text":"impl Octal for Pkcs7Flags","synthetic":false,"types":[]},{"text":"impl Octal for SslOptions","synthetic":false,"types":[]},{"text":"impl Octal for SslMode","synthetic":false,"types":[]},{"text":"impl Octal for SslVerifyMode","synthetic":false,"types":[]},{"text":"impl Octal for SslSessionCacheMode","synthetic":false,"types":[]},{"text":"impl Octal for ExtensionContext","synthetic":false,"types":[]},{"text":"impl Octal for ShutdownState","synthetic":false,"types":[]},{"text":"impl Octal for X509CheckFlags","synthetic":false,"types":[]}];
implementors["rug"] = [{"text":"impl Octal for Integer","synthetic":false,"types":[]},{"text":"impl Octal for Float","synthetic":false,"types":[]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()