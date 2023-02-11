use crate::errors::RustydomoError;
use zmq::Message;

pub struct MessageHelper {
    pub m: Message,
}

impl TryInto<u16> for MessageHelper {
    type Error = RustydomoError;

    fn try_into(self) -> Result<u16, Self::Error> {
        if let Some(content) = self.m.as_str() {
            // create fixed array to receive  the value to convert
            let mut fixed_array: [u8; 2] = Default::default();
            // prepare slice of u8
            let u8_content = content.as_bytes();
            // copy the 2 bytes to convert
            fixed_array.copy_from_slice(&u8_content[0..2]);
            // return the obtained value
            Ok(u16::from_ne_bytes(fixed_array))
        } else {
            Err(RustydomoError::ConversionError(
                "Failed to convert value to u16".to_string(),
            ))
        }
    }
}

impl TryInto<u32> for MessageHelper {
    type Error = RustydomoError;

    fn try_into(self) -> Result<u32, Self::Error> {
        if let Some(content) = self.m.as_str() {
            // create fixed array to receive  the value to convert
            let mut fixed_array: [u8; 4] = Default::default();
            // prepare slice of u8
            let u8_content = content.as_bytes();
            // copy the 2 bytes to convert
            fixed_array.copy_from_slice(&u8_content[0..4]);
            // return the obtained value
            Ok(u32::from_ne_bytes(fixed_array))
        } else {
            Err(RustydomoError::ConversionError(
                "Failed to convert value to u32".to_string(),
            ))
        }
    }
}
